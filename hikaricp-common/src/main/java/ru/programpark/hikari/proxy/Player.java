package ru.programpark.hikari.proxy;

import ru.programpark.hikari.pool.HikariPool;
import ru.programpark.hikari.util.CacheByteSource;
import ru.programpark.hikari.util.CacheCharSource;
import ru.programpark.hikari.util.FstHelper;
import lombok.Cleanup;
import lombok.SneakyThrows;
import org.apache.commons.beanutils.MethodUtils;

import java.lang.reflect.Method;
import java.sql.*;
import java.util.HashMap;
import java.util.Iterator;

public class Player implements AutoCloseable
{

   static int CallableStatementHashCode;
   static int ConnectionHashCode;
   static int PreparedStatementHashCode;
   static int StatementHashCode;

   static {
      try {
         CallableStatementHashCode = Class.forName("ru.programpark.hikari.proxy.CallableStatementJavassistProxy").hashCode();
         ConnectionHashCode = Class.forName("ru.programpark.hikari.proxy.ConnectionJavassistProxy").hashCode();
         PreparedStatementHashCode = Class.forName("ru.programpark.hikari.proxy.PreparedStatementJavassistProxy").hashCode();
         StatementHashCode = Class.forName("ru.programpark.hikari.proxy.StatementJavassistProxy").hashCode();
      } catch (ClassNotFoundException e) {
         e.printStackTrace();
      }
   }

   static Class[] emptyClassArray = new Class[]{};
   static Object[] emptyObjectArray = new Object[]{};

   private final PreparedStatement select;
   private final PreparedStatement delete;
   private HikariPool pool;
   private ConnectionProxy connectionProxy;
   private HashMap<Integer, ConnectionProxy> connections = new HashMap<Integer, ConnectionProxy>(10);
   private HashMap<Integer, Statement> statements = new HashMap<Integer, Statement>(30);

   @SneakyThrows
   public Player(HikariPool pool)
   {
      this.pool = pool;
      this.connectionProxy = pool.getConnection();
      connectionProxy.delegate2.setAutoCommit(false);
      select = connectionProxy.delegate2.prepareStatement("SELECT * FROM invocation_queue");
      delete = connectionProxy.delegate2.prepareStatement("DELETE FROM invocation_queue WHERE id = ?");
   }

   static Class[] argsClasses(Object[] args)
   {
      Class[] result = new Class[args.length];
      for (int i = 0; i < args.length; i++) {
         Object arg = args[i];
         result[i] = arg.getClass();
      }
      return result;
   }

   @SneakyThrows
   public void play()
   {
      @Cleanup ResultSet resultSet = select.executeQuery();
      while (resultSet.next()) {
         int id = resultSet.getInt(1);
         int connectionId = resultSet.getInt(2);
         int statementId = resultSet.getInt(3);
         int classHashCode = resultSet.getInt(4);
         String methodName = resultSet.getString(5);
         byte[] serializedArgs = resultSet.getBytes(6);

         Object[] args = serializedArgs == null ? null : (Object[]) FstHelper.fst.asObject(serializedArgs);

         Class clazz = ProxyFactory.classByHashCode(classHashCode);

         if (Connection.class.isAssignableFrom(clazz)) {

            ConnectionProxy connectionProxy = connections.get(connectionId);
            if (connectionProxy == null) {
               connectionProxy = pool.getConnection();
               connections.put(connectionId, connectionProxy);
            }
            Connection connection = connectionProxy.delegate;

            Method method = MethodUtils.getMatchingAccessibleMethod(clazz, methodName, args == null ? emptyClassArray : argsClasses(args));
            Object result = method.invoke(connection, args == null ? emptyObjectArray : args);

            if (statementId != 0)
               statements.put(statementId, (Statement) result);
            else if (connection.isClosed())
               connections.remove(connectionId);

         }

         else if (Statement.class.isAssignableFrom(clazz)) {
            Statement statement = statements.get(statementId);

            // todo generalize
            if (methodName.startsWith("set") && methodName.endsWith("Stream")) {
               assert args != null && args.length >= 1;
               if (args[1] instanceof CacheByteSource)
                  args[1] = ((CacheByteSource) args[1]).openStream();
               if (args[1] instanceof CacheCharSource)
                  args[1] = ((CacheCharSource) args[1]).openStream();
            }

            Method method = MethodUtils.getMatchingAccessibleMethod(clazz, methodName, args == null ? emptyClassArray : argsClasses(args));
            method.invoke(statement, args == null ? emptyObjectArray : args);

            if (statement.isClosed())
               statements.remove(statementId);

         }

         else {
            throw new AssertionError("should not reach here");
         }

         delete.setInt(1, id);
         delete.addBatch();
      }
      delete.executeBatch();
      connectionProxy.delegate2.commit();
   }

   static <T extends AutoCloseable> void closeAll(Iterable<T> iterable)
   {
      Iterator<T> iterator = iterable.iterator();
      while (iterator.hasNext()) {
         T item = iterator.next();
         try {
            item.close();
         }
         catch (Exception e) {
         }
         iterator.remove();
      }
   }

   @Override
   public void close() throws Exception
   {
      closeAll(statements.values());
      closeAll(connections.values());
      try {
         delete.close();
      }
      catch (SQLException e) {
      }
      try {
         select.close();
      }
      catch (SQLException e) {
      }
      try {
         connectionProxy.close();
      }
      catch (SQLException e) {
      }
   }
}
