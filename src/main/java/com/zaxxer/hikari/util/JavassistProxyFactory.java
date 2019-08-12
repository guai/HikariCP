/*
 * Copyright (C) 2013, 2014 Brett Wooldridge
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.zaxxer.hikari.util;

import com.zaxxer.hikari.pool.*;
import javassist.*;
import javassist.bytecode.ClassFile;
import lombok.SneakyThrows;

import java.io.IOException;
import java.lang.reflect.Array;
import java.sql.*;
import java.util.*;

/**
 * This class generates the proxy objects for {@link Connection}, {@link Statement},
 * {@link PreparedStatement}, and {@link CallableStatement}.  Additionally it injects
 * method bodies into the {@link ProxyFactory} class methods that can instantiate
 * instances of the generated proxies.
 *
 * @author Brett Wooldridge
 */
public final class JavassistProxyFactory
{
   private static ClassPool classPool;
   private static String genDirectory = "";

   public static void main(String... args) throws Exception {
      classPool = new ClassPool();
      classPool.importPackage("java.sql");
      classPool.appendClassPath(new LoaderClassPath(JavassistProxyFactory.class.getClassLoader()));

      if (args.length > 0) {
         genDirectory = args[0];
      }

      MethodBodyGenerator simpleMethodBodyGenerator = (method, superMethod) -> {
         boolean superDefined = (superMethod.getModifiers() & Modifier.ABSTRACT) == 0;
         boolean isThrowsSqlException = isThrowsSqlException(method);
         StringBuilder sb = new StringBuilder("{\n");
         if (superDefined)
            sb.append("return super.method($$);\n");
         else {
            if (isThrowsSqlException) {
               sb.append("	try {\n");
               sb.append("		return ((cast) delegate).method($$);\n");
               sb.append("	} catch (SQLException e) {\n");
               sb.append("		throw checkException(e);\n");
               sb.append("	}\n");
            } else
               sb.append("	return ((cast) delegate).method($$);\n");
         }
         sb.append("}\n");
         return sb.toString();
      };

      @SuppressWarnings("Convert2Lambda") MethodBodyGenerator compositeMethodBodyGenerator = new MethodBodyGenerator() {
         @Override
         @SneakyThrows
         public String generate(CtMethod method, CtMethod superMethod) {
            String name = method.getName();
            if (name.startsWith("get") && Character.isUpperCase(name.charAt(3)))
               return simpleMethodBodyGenerator.generate(method, superMethod);

            String descriptor = method.getMethodInfo().toString();
            boolean superDefined = (superMethod.getModifiers() & Modifier.ABSTRACT) == 0;
            boolean isThrowsSqlException = JavassistProxyFactory.isThrowsSqlException(method);
            StringBuilder sb = new StringBuilder("{\n");

            if (superDefined) {
               if (superMethod.getAnnotation(DontRecord.class) == null)
                  sb.append("	invoked(\"" + descriptor + "\", $args);\n");
               if (isThrowsSqlException) {
                  sb.append("	ReturnType result;\n");
                  sb.append("	try {\n");
                  sb.append("		result = super.method($$);\n");
                  sb.append("	} catch (SQLException e) {\n");
                  sb.append("		throw checkException(e);\n");
                  sb.append("	}\n");
                  sb.append("	return result;\n");
               } else
                  sb.append("return super.method($$);\n");
            } else {
               sb.append("	invoked(\"" + descriptor + "\", $args);\n");
               if (isThrowsSqlException) {
                  sb.append("	ReturnType result;\n");
                  sb.append("	try {\n");
                  sb.append("		result = ((cast) delegate).method($$);\n");
                  sb.append("	} catch (SQLException e) {\n");
                  sb.append("		throw checkException(e);\n");
                  sb.append("	}\n");
                  sb.append("	if(!isFallbackMode())");
                  sb.append("		try {\n");
                  sb.append("			((cast) twinDelegate).method($$);\n");
                  sb.append("		} catch (SQLException e) {\n");
                  sb.append("			checkTwinException(e);\n");
                  sb.append("		}\n");
                  sb.append("	return result;\n");
               } else {
                  sb.append("	ReturnType result;\n");
                  sb.append("	result = ((cast) delegate).method($$);\n");
                  sb.append("	if(!isFallbackMode())");
                  sb.append("		((cast) twinDelegate).method($$);\n");
                  sb.append("	return result;\n");
               }
            }
            sb.append("}\n");
            return sb.toString();
         }
      };

      generateProxyClass(Connection.class, ProxyConnection.class.getName(), compositeMethodBodyGenerator);
      generateProxyClass(Statement.class, ProxyStatement.class.getName(), compositeMethodBodyGenerator);
      generateProxyClass(PreparedStatement.class, ProxyPreparedStatement.class.getName(), compositeMethodBodyGenerator);
      generateProxyClass(CallableStatement.class, ProxyCallableStatement.class.getName(), compositeMethodBodyGenerator);

      generateProxyClass(ResultSet.class, ProxyResultSet.class.getName(), simpleMethodBodyGenerator);

      modifyProxyFactory();
   }

   private static void modifyProxyFactory() throws NotFoundException, CannotCompileException, IOException {
      System.out.println("Generating method bodies for com.zaxxer.hikari.proxy.ProxyFactory");

      CtClass proxyCt = classPool.getCtClass(ProxyFactory.class.getCanonicalName());

      for (CtMethod method : proxyCt.getMethods()) {
         String name = method.getName();
         if (name.startsWith("getProxy")) {
            CtClass returnType = method.getReturnType();
            String proxyClassName = returnType.getPackageName() + ".Hikari" + returnType.getSimpleName();
            try {
               method.setBody("{return new " + proxyClassName + "($$);}");
            } catch (Exception e) {
               e.printStackTrace();
            }
         }
      }

      proxyCt.writeFile(genDirectory + "target/classes");
   }

   /**
    *  Generate Javassist Proxy Classes
    */
   private static <T> void generateProxyClass(Class<T> primaryInterface, String superClassName, MethodBodyGenerator methodBodyGenerator) throws Exception
   {
      String newClassName = superClassName.replaceAll("(.+)\\.(\\w+)", "$1.Hikari$2");

      CtClass superCt = classPool.getCtClass(superClassName);
      CtClass targetCt = classPool.makeClass(newClassName, superCt);
      targetCt.setModifiers(Modifier.FINAL);

      System.out.println("Generating " + newClassName);

      targetCt.setModifiers(Modifier.PUBLIC);

      // Make a set of method signatures we inherit implementation for, so we don't generate delegates for these
      Set<String> superSigs = new HashSet<>();
      for (CtMethod method : superCt.getMethods()) {
         if ((method.getModifiers() & Modifier.FINAL) == Modifier.FINAL) {
            superSigs.add(method.getName() + method.getSignature());
         }
      }

      Set<String> methods = new HashSet<>();
      for (Class<?> intf : getAllInterfaces(primaryInterface)) {
         CtClass intfCt = classPool.getCtClass(intf.getName());
         targetCt.addInterface(intfCt);
         for (CtMethod intfMethod : intfCt.getDeclaredMethods()) {
            final String signature = intfMethod.getName() + intfMethod.getSignature();

            // don't generate delegates for methods we override
            if (superSigs.contains(signature)) {
               continue;
            }

            // Ignore already added methods that come from other interfaces
            if (methods.contains(signature)) {
               continue;
            }

            // Track what methods we've added
            methods.add(signature);

            // If the super-Proxy has concrete methods (non-abstract), transform the call into a simple super.method() call
            CtMethod superMethod = superCt.getMethod(intfMethod.getName(), intfMethod.getSignature());

            String body = methodBodyGenerator.generate(intfMethod, superMethod);

            body = body.replace("method", intfMethod.getName());

            body = body.replace("cast", primaryInterface.getName());

            if (intfMethod.getReturnType() == CtClass.voidType) {
               body = body.replace("return result;", "");
               body = body.replace("return", "");
               body = body.replace("ReturnType result;", "");
               body = body.replace("ReturnType result = ", "");
               body = body.replace("result = ", "");
            } else
               body = body.replace("ReturnType", intfMethod.getReturnType().getName());

            try {
               // Clone the method we want to inject into
               CtMethod method = CtNewMethod.copy(intfMethod, targetCt, null);
               method.setBody(body);
               targetCt.addMethod(method);
            } catch (Exception e) {
               e.printStackTrace();
            }

         }
      }

      targetCt.getClassFile().setMajorVersion(ClassFile.JAVA_8);
      targetCt.writeFile(genDirectory + "target/classes");
   }

   private static boolean isThrowsSqlException(CtMethod method)
   {
      try {
         for (CtClass clazz : method.getExceptionTypes()) {
            if (clazz.getSimpleName().equals("SQLException")) {
               return true;
            }
         }
      }
      catch (NotFoundException e) {
         // fall thru
      }

      return false;
   }

   private static boolean isDefaultMethod(Class<?> intf, CtMethod intfMethod) throws Exception
   {
      List<Class<?>> paramTypes = new ArrayList<>();

      for (CtClass pt : intfMethod.getParameterTypes()) {
         paramTypes.add(toJavaClass(pt));
      }

      return intf.getDeclaredMethod(intfMethod.getName(), paramTypes.toArray(new Class[0])).toString().contains("default ");
   }

   private static Set<Class<?>> getAllInterfaces(Class<?> clazz)
   {
      Set<Class<?>> interfaces = new LinkedHashSet<>();
      for (Class<?> intf : clazz.getInterfaces()) {
         if (intf.getInterfaces().length > 0) {
            interfaces.addAll(getAllInterfaces(intf));
         }
         interfaces.add(intf);
      }
      if (clazz.getSuperclass() != null) {
         interfaces.addAll(getAllInterfaces(clazz.getSuperclass()));
      }

      if (clazz.isInterface()) {
         interfaces.add(clazz);
      }

      return interfaces;
   }

   private static Class<?> toJavaClass(CtClass cls) throws Exception
   {
      if (cls.getName().endsWith("[]")) {
         return Array.newInstance(toJavaClass(cls.getName().replace("[]", "")), 0).getClass();
      }
      else {
         return toJavaClass(cls.getName());
      }
   }

   private static Class<?> toJavaClass(String cn) throws Exception
   {
      switch (cn) {
      case "int":
         return int.class;
      case "long":
         return long.class;
      case "short":
         return short.class;
      case "byte":
         return byte.class;
      case "float":
         return float.class;
      case "double":
         return double.class;
      case "boolean":
         return boolean.class;
      case "char":
         return char.class;
      case "void":
         return void.class;
      default:
         return Class.forName(cn);
      }
   }

   interface MethodBodyGenerator {
      String generate(CtMethod method, CtMethod superMethod);
   }
}
