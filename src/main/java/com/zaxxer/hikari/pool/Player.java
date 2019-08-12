package com.zaxxer.hikari.pool;

import com.google.common.base.Splitter;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import com.google.common.io.Resources;
import com.zaxxer.hikari.util.Marshaller;
import javassist.runtime.Desc;
import lombok.Cleanup;
import lombok.SneakyThrows;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.reflect.MethodUtils;

import java.lang.reflect.Method;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.HashMap;
import java.util.Iterator;

class Player implements AutoCloseable {

   private static Table<Class, String, Method> methods = HashBasedTable.create();

   private final PreparedStatement select;
   private final PreparedStatement delete;
   private HikariPool pool;
   private Connection twinConnection;
   private HashMap<Integer, Connection> connections = new HashMap<>(10);
   private HashMap<Integer, Statement> statements = new HashMap<>(10);

   @SneakyThrows
   private static void createInvocationQueueTable(Connection connection) {
      connection.setAutoCommit(false);
      DatabaseMetaData metaData = connection.getMetaData();
      if (!invocationQueueTableExists(metaData)) {
         @Cleanup Statement statement = connection.createStatement();
         String db = metaData.getDatabaseProductName().toLowerCase();
         URL resource = Resources.getResource("create-invocation_queue-" + db + ".sql");
         String sqls = Resources.asCharSource(resource, StandardCharsets.UTF_8).read();
         for (String sql : Splitter.on("\n\n").omitEmptyStrings().split(sqls)) {
            sql = sql.trim();
            if (sql.endsWith(";"))
               sql = sql.substring(0, sql.length() - 1);
            statement.execute(sql);
         }
         connection.commit();
      }
   }

   @SneakyThrows
   private static boolean invocationQueueTableExists(DatabaseMetaData metaData) {
      @Cleanup ResultSet tables = metaData.getTables(null, null, "invocation_queue", new String[]{"TABLE"});
      @Cleanup ResultSet TABLES = metaData.getTables(null, null, "INVOCATION_QUEUE", new String[]{"TABLE"});
      return tables.next() || TABLES.next();
   }

   @SneakyThrows
   Player(HikariPool pool) {
      this.pool = pool;
      @Cleanup Connection connection = pool.getUnwrappedDataSource().getConnection();
      connection.setAutoCommit(false);
      createInvocationQueueTable(connection);
      try {
         twinConnection = pool.config.getTwinDataSource().getConnection(); // no @Cleanup
         twinConnection.setAutoCommit(false);
      } catch (SQLException ignore) {
      }
      if (twinConnection == null || !invocationQueueTableExists(twinConnection.getMetaData())) {
         select = delete = null;
         return;
      }
      select = twinConnection.prepareStatement("SELECT * FROM invocation_queue ORDER BY id");
      delete = twinConnection.prepareStatement("DELETE FROM invocation_queue WHERE id = ?");
   }

   static Method getMethod(Class clazz, String method) {
      Method result = methods.get(clazz, method);
      if (result != null) return result;
      Iterator<String> split = Splitter.on(' ').limit(2).split(method).iterator();
      String methodName = split.next();
      Class<?>[] sig = Desc.getParams(split.next());
      result = MethodUtils.getMatchingAccessibleMethod(clazz, methodName, sig);
      methods.put(clazz, method, result);
      return result;
   }

   @SneakyThrows
   boolean play() {
      if (select == null) return false;

      @Cleanup ResultSet resultSet = select.executeQuery();
      while (resultSet.next()) {
         int id = resultSet.getInt(1);
         int connectionId = resultSet.getInt(2);
         int statementId = resultSet.getInt(3);
         char classId = resultSet.getString(4).charAt(0);
         String method = resultSet.getString(5);
         byte[] serializedArgs = resultSet.getBytes(6);

         switch (classId) {
            case 'C':
               Connection connection = connections.get(connectionId);
               Method connectionMethod = getMethod(Connection.class, method);

               if (connection == null) {
                  if (connectionMethod.getName().equals("close")) break;
                  connection = pool.getUnwrappedDataSource().getConnection();
                  connection.setAutoCommit(false);
                  connections.put(connectionId, connection);
               }

               Object result = connectionMethod.invoke(connection, Marshaller.fromBytes(serializedArgs));

               if (statementId != 0)
                  statements.put(statementId, (Statement) result);
               else if (connection.isClosed())
                  connections.remove(connectionId);

               break;
            case 'R':
               throw new NotImplementedException("ResultSet not implemented yet");
            default:
               Class clazz;
               switch (classId) {
                  case 'S':
                     clazz = Statement.class;
                     break;
                  case 'P':
                     clazz = PreparedStatement.class;
                     break;
                  case 'X':
                     clazz = CallableStatement.class;
                     break;
                  default:
                     throw new RuntimeException("Must not reach");
               }
               Statement statement = statements.get(statementId);
               Method statementMethod = getMethod(clazz, method);

               if (statement == null && statementMethod.getName().equals("close")) break;

               statementMethod.invoke(statement, Marshaller.fromBytes(serializedArgs));

               if (statement.isClosed())
                  statements.remove(statementId);

               break;
         }

         delete.setInt(1, id);
         delete.addBatch();
      }
      delete.executeBatch();
      twinConnection.commit();

      for (Statement statement : statements.values())
         statement.close();
      statements.clear();
      for (Connection connection : connections.values())
         connection.close();
      connections.clear();

      return true;
   }

   private static void close(AutoCloseable what) {
      if (what == null)
         return;
      try {
         what.close();
      } catch (Exception e) {
      }
   }

   private static <T extends AutoCloseable> void closeAll(Iterable<T> iterable) {
      Iterator<T> iterator = iterable.iterator();
      while (iterator.hasNext()) {
         T item = iterator.next();
         close(item);
         iterator.remove();
      }
   }

   @Override
   public void close() {
      closeAll(statements.values());
      closeAll(connections.values());
      close(delete);
      close(select);
      close(twinConnection);
   }
}
