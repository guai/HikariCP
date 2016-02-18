package ru.programpark.hikari.proxy;

import com.google.common.base.Splitter;
import com.google.common.io.Resources;
import lombok.Cleanup;
import lombok.SneakyThrows;
import org.apache.commons.lang3.reflect.MethodUtils;
import ru.programpark.hikari.pool.HikariPool;
import ru.programpark.hikari.util.Serializator;

import java.lang.reflect.Method;
import java.net.URL;
import java.nio.charset.Charset;
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
   private HashMap<Integer, Connection> connections = new HashMap<Integer, Connection>(10);
   private HashMap<Integer, Statement> statements = new HashMap<Integer, Statement>(30);

   @SneakyThrows
   private static boolean createInvocationQueueTable(Connection connection)
   {
      connection.setAutoCommit(false);
      DatabaseMetaData metaData = connection.getMetaData();
      @Cleanup ResultSet tables = metaData.getTables(null, null, "invocation_queue", new String[] { "TABLE" });
      @Cleanup ResultSet TABLES = metaData.getTables(null, null, "INVOCATION_QUEUE", new String[] { "TABLE" });

      if (tables.next() || TABLES.next())
         return false;
      else {
         @Cleanup Statement statement = connection.createStatement();
         String db = metaData.getDatabaseProductName().toLowerCase();
         URL resource = Resources.getResource("create-invocation_queue-" + db + ".sql");
         String sqls = Resources.asCharSource(resource, Charset.forName("UTF-8")).read();
         for (String sql : Splitter.on("\n\n").omitEmptyStrings().split(sqls)) {
            sql = sql.trim();
            if (sql.endsWith(";"))
               sql = sql.substring(0, sql.length() - 1);
            statement.execute(sql);
         }
         connection.commit();
         return true;
      }
   }

   @SneakyThrows
   public Player(HikariPool pool)
   {
      this.pool = pool;
      this.connectionProxy = pool.getConnection();
      createInvocationQueueTable(connectionProxy.delegate);
      if(connectionProxy.delegate2 == null || createInvocationQueueTable(connectionProxy.delegate2)) {
         select = delete = null;
         return;
      }
      select = connectionProxy.delegate2.prepareStatement("SELECT * FROM invocation_queue ORDER BY id");
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
   public boolean play()
   {
      if(select == null)
         return false;
      @Cleanup ResultSet resultSet = select.executeQuery();
      while (resultSet.next()) {
         int id = resultSet.getInt(1);
         int connectionId = resultSet.getInt(2);
         int statementId = resultSet.getInt(3);
         int classHashCode = resultSet.getInt(4);
         String methodName = resultSet.getString(5);
         byte[] serializedArgs = resultSet.getBytes(6);

         Object[] args = serializedArgs == null ? null : Serializator.fromBytes(serializedArgs);

         Class clazz = ProxyFactory.classByHashCode(classHashCode);

         if (Connection.class.isAssignableFrom(clazz)) {

            Connection connection = connections.get(connectionId);
            if (connection == null) {
               connection = pool.getDataSource().getConnection();
               connection.setAutoCommit(false);
               connections.put(connectionId, connection);
            }

            Method method = MethodUtils.getMatchingAccessibleMethod(clazz, methodName, args == null ? emptyClassArray : argsClasses(args));
            Object result = method.invoke(connection, args == null ? emptyObjectArray : args);

            if (statementId != 0)
               statements.put(statementId, (Statement) result);
            else if (connection.isClosed())
               connections.remove(connectionId);

         }

         else if (Statement.class.isAssignableFrom(clazz)) {
            Statement statement = statements.get(statementId);

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
      return true;
   }

   static void close(AutoCloseable what) {
      if(what == null)
         return;
      try {
         what.close();
      } catch (Exception e) {
      }
   }

   static <T extends AutoCloseable> void closeAll(Iterable<T> iterable)
   {
      Iterator<T> iterator = iterable.iterator();
      while (iterator.hasNext()) {
         T item = iterator.next();
         close(item);
         iterator.remove();
      }
   }

   @Override
   public void close() throws Exception
   {
      closeAll(statements.values());
      closeAll(connections.values());
      close(delete);
      close(select);
      close(connectionProxy);
   }
}
