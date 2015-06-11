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

package com.zaxxer.hikari.proxy;

import com.zaxxer.hikari.util.ClassLoaderUtils;
import javassist.*;
import lombok.SneakyThrows;

import java.lang.reflect.Array;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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
   private ClassPool classPool;

   static {
      ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
      try {
         Thread.currentThread().setContextClassLoader(JavassistProxyFactory.class.getClassLoader());

         JavassistProxyFactory proxyFactoryFactory = new JavassistProxyFactory();
         proxyFactoryFactory.modifyProxyFactory();
      }
      catch (Exception e) {
         throw new RuntimeException("Fatal exception during proxy generation", e);
      }
      finally {
         Thread.currentThread().setContextClassLoader(contextClassLoader);
      }
   }

   /**
    * Simply invoking this method causes the initialization of this class.  All work
    * by this class is performed in static initialization.
    */
   public static void initialize()
   {
      // no-op
   }

   private JavassistProxyFactory() throws Exception
   {
      classPool = new ClassPool();
      classPool.importPackage("java.sql");
      classPool.appendClassPath(new LoaderClassPath(this.getClass().getClassLoader()));

      MethodBodyGenerator simpleMethodBodyGenerator = (method, superMethod) -> {
         boolean superDefined = (superMethod.getModifiers() & Modifier.ABSTRACT) == 0;
         boolean isThrowsSqlException = isThrowsSqlException(method);
         StringBuilder sb = new StringBuilder("{\n");
//			sb.append("	invoked(\"method\", $args);\n");
         if (superDefined)
            sb.append("return super.method($$);\n");
         else {
            if (isThrowsSqlException) {
               sb.append("	try {\n");
               sb.append("		return ((cast) delegate).method($$);\n");
               sb.append("	} catch (SQLException e) {\n");
               sb.append("		throw checkException(e);\n");
               sb.append("	}\n");
            }
            else
               sb.append("	return ((cast) delegate).method($$);\n");
         }
         sb.append("}\n");
         return sb.toString();
      };

      MethodBodyGenerator compositeMethodBodyGenerator = new MethodBodyGenerator()
      {
         @Override
         @SneakyThrows
         public String generate(CtMethod method, CtMethod superMethod)
         {
            String name = method.getName();
            if (name.startsWith("get") && Character.isUpperCase(name.charAt(3)))
               return simpleMethodBodyGenerator.generate(method, superMethod);

            boolean superDefined = (superMethod.getModifiers() & Modifier.ABSTRACT) == 0;
            boolean isThrowsSqlException = JavassistProxyFactory.this.isThrowsSqlException(method);
            StringBuilder sb = new StringBuilder("{\n");


            if (superDefined) {
               if (superMethod.getAnnotation(DontRecord.class) == null)
                  sb.append("	invoked(\"method\", $args);\n");
               if (isThrowsSqlException) {
                  sb.append("	ReturnType result;\n");
                  sb.append("	try {\n");
                  sb.append("		result = super.method($$);\n");
                  sb.append("	} catch (SQLException e) {\n");
                  sb.append("		throw checkException(e);\n");
                  sb.append("	}\n");
                  sb.append("	return result;\n");
               }
               else
                  sb.append("return super.method($$);\n");
            }
            else {
               sb.append("	invoked(\"method\", $args);\n");
               if (isThrowsSqlException) {
                  sb.append("	ReturnType result;\n");
                  sb.append("	try {\n");
                  sb.append("		result = ((cast) delegate).method($$);\n");
                  sb.append("	} catch (SQLException e) {\n");
                  sb.append("		throw checkException(e);\n");
                  sb.append("	}\n");
                  sb.append("	if(!isFallbackMode())");
                  sb.append("		try {\n");
                  sb.append("			((cast) delegate2).method($$);\n");
                  sb.append("		} catch (SQLException e) {\n");
                  sb.append("			checkException2(e);\n");
                  sb.append("		}\n");
                  sb.append("	return result;\n");
               }
               else {
                  sb.append("	ReturnType result;\n");
                  sb.append("	result = ((cast) delegate).method($$);\n");
                  sb.append("	if(!isFallbackMode())");
                  sb.append("		((cast) delegate2).method($$);\n");
                  sb.append("	return result;\n");
               }
            }
            sb.append("}\n");
            return sb.toString();
         }
      };

      generateProxyClass(Connection.class, ConnectionProxy.class, compositeMethodBodyGenerator);
      generateProxyClass(Statement.class, StatementProxy.class, compositeMethodBodyGenerator);
      generateProxyClass(PreparedStatement.class, PreparedStatementProxy.class, compositeMethodBodyGenerator);
      generateProxyClass(CallableStatement.class, CallableStatementProxy.class, compositeMethodBodyGenerator);

      generateProxyClass(ResultSet.class, ResultSetProxy.class, simpleMethodBodyGenerator);
   }

   private void modifyProxyFactory() throws Exception
   {
      CtClass proxyCt = classPool.getCtClass("com.zaxxer.hikari.proxy.ProxyFactory");

      for (CtMethod method : proxyCt.getMethods()) {
         String name = method.getName();
         if (name.startsWith("get") && name.contains("Proxy")) {
            String proxyClassName = method.getReturnType().getName().replace("Proxy", "JavassistProxy");
            try {
               method.setBody("{return new " + proxyClassName + "($$);}");
            }
            catch (Exception e) {
               e.printStackTrace();
               // todo remove
            }
         }
      }

      proxyCt.toClass(classPool.getClassLoader(), getClass().getProtectionDomain());
   }

   /**
    *  Generate Javassist Proxy Classes
    */
   @SuppressWarnings("unchecked")
   private <T> Class<T> generateProxyClass(Class<T> primaryInterface, Class<?> superClass, MethodBodyGenerator methodBodyGenerator) throws Exception
   {
      // Make a new class that extends one of the JavaProxy classes (ie. superClass); use the name to XxxJavassistProxy instead of XxxProxy
      String superClassName = superClass.getName();
      CtClass superClassCt = classPool.getCtClass(superClassName);
      CtClass targetCt = classPool.makeClass(superClassName.replace("Proxy", "JavassistProxy"), superClassCt);
      targetCt.setModifiers(Modifier.FINAL);

      // Make a set of method signatures we inherit implementation for, so we don't generate delegates for these
      Set<String> superSigs = new HashSet<String>();
      for (CtMethod method : superClassCt.getMethods()) {
         if ((method.getModifiers() & Modifier.FINAL) == Modifier.FINAL) {
            superSigs.add(method.getName() + method.getSignature());
         }
      }

      Set<String> methods = new HashSet<String>();
      Set<Class<?>> interfaces = ClassLoaderUtils.getAllInterfaces(primaryInterface);
      for (Class<?> intf : interfaces) {
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

            // Ignore default methods (only for Jre8 or later)
            if (isDefaultMethod(intf, intfCt, intfMethod)) {
               continue;
            }

            // Track what methods we've added
            methods.add(signature);

            // If the super-Proxy has concrete methods (non-abstract)
            CtMethod superMethod = superClassCt.getMethod(intfMethod.getName(), intfMethod.getSignature());

            String body = methodBodyGenerator.generate(intfMethod, superMethod);

            body = body.replace("method", intfMethod.getName());

            body = body.replace("cast", primaryInterface.getName());

            if (intfMethod.getReturnType() == CtClass.voidType) {
               body = body.replace("return result;", "");
               body = body.replace("return", "");
               body = body.replace("ReturnType result;", "");
               body = body.replace("ReturnType result = ", "");
               body = body.replace("result = ", "");
            }
            else
               body = body.replace("ReturnType", intfMethod.getReturnType().getName());

            try {
               // Clone the method we want to inject into
               CtMethod method = CtNewMethod.copy(intfMethod, targetCt, null);
               method.setBody(body);
               targetCt.addMethod(method);
            }
            catch (Exception e) {
               e.printStackTrace();
               //todo remove
            }

         }
      }

      return targetCt.toClass(classPool.getClassLoader(), getClass().getProtectionDomain());
   }

   private boolean isThrowsSqlException(CtMethod method)
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

   private boolean isDefaultMethod(Class<?> intf, CtClass intfCt, CtMethod intfMethod) throws Exception
   {
      List<Class<?>> paramTypes = new ArrayList<Class<?>>();

      for (CtClass pt : intfMethod.getParameterTypes()) {
         paramTypes.add(toJavaClass(pt));
      }

      return intf.getDeclaredMethod(intfMethod.getName(), paramTypes.toArray(new Class[0])).toString().contains("default ");
   }

   private Class<?> toJavaClass(CtClass cls) throws Exception
   {
      if (cls.getName().endsWith("[]")) {
         return Array.newInstance(toJavaClass(cls.getName().replace("[]", "")), 0).getClass();
      }
      else {
         return toJavaClass(cls.getName());
      }
   }

   private Class<?> toJavaClass(String cn) throws Exception
   {
		switch(cn) {
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

	public interface MethodBodyGenerator {
		String generate(CtMethod method, CtMethod superMethod);
	}
}
