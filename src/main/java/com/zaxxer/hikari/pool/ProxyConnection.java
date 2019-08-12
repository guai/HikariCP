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

package com.zaxxer.hikari.pool;

import com.zaxxer.hikari.util.DontRecord;
import com.zaxxer.hikari.util.FastList;
import com.zaxxer.hikari.util.Marshaller;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.sql.*;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;

import static com.zaxxer.hikari.util.ClockSource.currentTime;

/**
 * This is the proxy class for java.sql.Connection.
 *
 * @author Brett Wooldridge
 */
public abstract class ProxyConnection implements Connection
{
   static final int DIRTY_BIT_READONLY   = 0b000001;
   static final int DIRTY_BIT_AUTOCOMMIT = 0b000010;
   static final int DIRTY_BIT_ISOLATION  = 0b000100;
   static final int DIRTY_BIT_CATALOG    = 0b001000;
   static final int DIRTY_BIT_NETTIMEOUT = 0b010000;
   static final int DIRTY_BIT_SCHEMA     = 0b100000;

   private static final Logger LOGGER;
   private static final Set<String> ERROR_STATES;
   private static final Set<Integer> ERROR_CODES;

   @SuppressWarnings("WeakerAccess")
   protected Connection delegate;

   private final PoolEntry poolEntry;
   private final ProxyLeakTask leakTask;
   private final FastList<ProxyStatement> openStatements;

   private int dirtyBits;
   private long lastAccess;
   private boolean isCommitStateDirty;

   private boolean isReadOnly;
   private boolean isAutoCommit;
   private int networkTimeout;
   private int transactionIsolation;
   private String dbcatalog;
   private String dbschema;

   // ha
   final ConcurrentLinkedQueue<Record> invocationQueue;
   Connection twinDelegate = null;
   private Record tailRecord;
   private PreparedStatement fallbackInsert;

   // static initializer
   static {
      LOGGER = LoggerFactory.getLogger(ProxyConnection.class);

      ERROR_STATES = new HashSet<>();
      ERROR_STATES.add("0A000"); // FEATURE UNSUPPORTED
      ERROR_STATES.add("57P01"); // ADMIN SHUTDOWN
      ERROR_STATES.add("57P02"); // CRASH SHUTDOWN
      ERROR_STATES.add("57P03"); // CANNOT CONNECT NOW
      ERROR_STATES.add("01002"); // SQL92 disconnect error
      ERROR_STATES.add("JZ0C0"); // Sybase disconnect error
      ERROR_STATES.add("JZ0C1"); // Sybase disconnect error

      ERROR_CODES = new HashSet<>();
      ERROR_CODES.add(500150);
      ERROR_CODES.add(2399);
   }

   protected ProxyConnection(final PoolEntry poolEntry, final Connection connection, final FastList<ProxyStatement> openStatements, final ProxyLeakTask leakTask, final long now, final boolean isReadOnly, final boolean isAutoCommit) {
      this.poolEntry = poolEntry;
      this.delegate = connection;
      this.openStatements = openStatements;
      this.leakTask = leakTask;
      this.lastAccess = now;
      this.isReadOnly = isReadOnly;
      this.isAutoCommit = isAutoCommit;

      HikariPool pool = poolEntry.hikariPool;
      // already under lock
      if (!pool.fallback)
         try {
            this.twinDelegate = pool.config.getTwinDataSource().getConnection();
            pool.setupConnection(this.twinDelegate);
         } catch (SQLException | PoolBase.ConnectionSetupException e) {
            pool.fallback = true;
            this.twinDelegate = null;
            LOGGER.warn("twin connection failed", e);
         }
      this.invocationQueue = new ConcurrentLinkedQueue<>();
   }

   /** {@inheritDoc} */
   @Override
   public final String toString()
   {
      return this.getClass().getSimpleName() + '@' + System.identityHashCode(this) + " wrapping " + delegate;
   }

   public char getClassId() {
      return 'C';
   }

   // ***********************************************************************
   //                     Connection State Accessors
   // ***********************************************************************

   final boolean getAutoCommitState()
   {
      return isAutoCommit;
   }

   final String getCatalogState()
   {
      return dbcatalog;
   }

   final String getSchemaState()
   {
      return dbschema;
   }

   final int getTransactionIsolationState()
   {
      return transactionIsolation;
   }

   final boolean getReadOnlyState()
   {
      return isReadOnly;
   }

   final int getNetworkTimeoutState()
   {
      return networkTimeout;
   }

   // ***********************************************************************
   //                          Internal methods
   // ***********************************************************************

   final PoolEntry getPoolEntry()
   {
      return poolEntry;
   }

   final SQLException checkException(SQLException sqle)
   {
      SQLException nse = sqle;
      for (int depth = 0; delegate != ClosedConnection.CLOSED_CONNECTION && nse != null && depth < 10; depth++) {
         final String sqlState = nse.getSQLState();
         if (sqlState != null && sqlState.startsWith("08")
             || nse instanceof SQLTimeoutException
             || ERROR_STATES.contains(sqlState)
             || ERROR_CODES.contains(nse.getErrorCode())) {

            // broken connection
            LOGGER.warn("{} - Connection {} marked as broken because of SQLSTATE({}), ErrorCode({})",
                        poolEntry.getPoolName(), delegate, sqlState, nse.getErrorCode(), nse);
            leakTask.cancel();
            poolEntry.evict("(connection is broken)");
            delegate = ClosedConnection.CLOSED_CONNECTION;
         }
         else {
            nse = nse.getNextException();
         }
      }

      return sqle;
   }

   public final void checkTwinException(final SQLException sqle) throws SQLException {
      if (LOGGER.isTraceEnabled())
         LOGGER.trace(getClass().getName() + ".checkTwinException", sqle);
      poolEntry.hikariPool.fallback();
      if (twinDelegate != null) {
         try {
            twinDelegate.close();
         } catch (SQLException e) {
            LOGGER.error("Failed to close twin connection", e);
         }
         twinDelegate = null;
      }
      drainQueue();
   }

   final boolean isFallbackMode() {
      return twinDelegate == null;
   }

   private void drainQueue() throws SQLException {
      try {
         if (fallbackInsert == null)
            fallbackInsert = delegate.prepareStatement("INSERT INTO invocation_queue (connection_id, statement_id, class, method, args) VALUES (?, ?, ?, ?, ?)");

         for (ProxyStatement openStatement : openStatements)
            openStatement.drainQueue();

         int i = 0;
         Iterator<Record> iterator = invocationQueue.iterator();
         while (iterator.hasNext()) {
            Record record = iterator.next();

            fallbackInsert.setInt(1, record.connectionId);
            fallbackInsert.setInt(2, record.statementId);
            fallbackInsert.setString(3, String.valueOf(record.classId));
            fallbackInsert.setString(4, record.method);
            if (record.args.length == 0)
               fallbackInsert.setNull(5, Types.NULL);
            else
               fallbackInsert.setBytes(5, Marshaller.toBytes(record.args));

            fallbackInsert.addBatch();
            iterator.remove();
            i++;
         }
         if (i > 0)
            fallbackInsert.executeBatch();
         if (!delegate.getAutoCommit())
            delegate.commit();
      } catch (SQLException e) {
         throw checkException(e);
      }
   }

   private int getConnectionId() {
      return System.identityHashCode(this);
   }

   private int getStatementId() {
      return 0;
   }

   private void clearSuccessful() {
      Iterator<Record> iterator = invocationQueue.iterator();
      while (iterator.hasNext())
         if (iterator.next().statementId != 0)
            iterator.remove();
   }

   @SuppressWarnings("WeakerAccess")
   protected final void invoked(String method, Object[] args) {
      invocationQueue.add(tailRecord = new Record(getConnectionId(), getStatementId(), getClassId(), method, args));
      if (LOGGER.isTraceEnabled())
         LOGGER.trace(tailRecord.toString());
   }

   final synchronized void untrackStatement(final ProxyStatement statement)
   {
      openStatements.remove(statement);
   }

   final void markCommitStateDirty()
   {
      if (isAutoCommit) {
         lastAccess = currentTime();
      }
      else {
         isCommitStateDirty = true;
      }
   }

   void cancelLeakTask()
   {
      leakTask.cancel();
   }

   private synchronized <T extends Statement> T trackStatement(final T statement)
   {
      openStatements.add((ProxyStatement) statement);

      return statement;
   }

   @SuppressWarnings("EmptyTryBlock")
   private synchronized void closeStatements()
   {
      final int size = openStatements.size();
      if (size > 0) {
         for (int i = 0; i < size && delegate != ClosedConnection.CLOSED_CONNECTION; i++) {
            try (Statement ignored = openStatements.get(i)) {
               // automatic resource cleanup
            }
            catch (SQLException e) {
               LOGGER.warn("{} - Connection {} marked as broken because of an exception closing open statements during Connection.close()",
                           poolEntry.getPoolName(), delegate);
               leakTask.cancel();
               poolEntry.evict("(exception closing Statements during Connection.close())");
               delegate = ClosedConnection.CLOSED_CONNECTION;
            }
         }

         openStatements.clear();
      }
   }

   // **********************************************************************
   //              "Overridden" java.sql.Connection Methods
   // **********************************************************************

   /** {@inheritDoc} */
   @Override
   public final void close() throws SQLException
   {
      // Closing statements can cause connection eviction, so this must run before the conditional below
      closeStatements();

      if (delegate != ClosedConnection.CLOSED_CONNECTION) {
         leakTask.cancel();

         try {
            if (isCommitStateDirty && !isAutoCommit) {
               delegate.rollback();
               lastAccess = currentTime();
               LOGGER.debug("{} - Executed rollback on connection {} due to dirty commit state on close().", poolEntry.getPoolName(), delegate);
               if (!isFallbackMode())
                  try {
                     twinDelegate.rollback();
                  } catch (SQLException e) {
                     checkTwinException(e);
                  }
            } else if (isFallbackMode()) {
               invoked("close ()V", Marshaller.emptyObjectArray);
               drainQueue();
            }

            if (dirtyBits != 0) {
               poolEntry.resetConnectionState(this, dirtyBits);
               lastAccess = currentTime();
            }

            delegate.clearWarnings();
            if (!isFallbackMode())
               try {
                  twinDelegate.clearWarnings();
               } catch (SQLException e) {
                  checkTwinException(e);
               }
         }
         catch (SQLException e) {
            // when connections are aborted, exceptions are often thrown that should not reach the application
            if (!poolEntry.isMarkedEvicted()) {
               throw checkException(e);
            }
         }
         finally {
            delegate = ClosedConnection.CLOSED_CONNECTION;
            twinDelegate = null;
            poolEntry.recycle(lastAccess);
         }
      }
   }

   /** {@inheritDoc} */
   @Override
   @SuppressWarnings("RedundantThrows")
   @DontRecord
   public boolean isClosed() throws SQLException
   {
      return (delegate == ClosedConnection.CLOSED_CONNECTION);
   }

   /** {@inheritDoc} */
   @Override
   public Statement createStatement() throws SQLException
   {
      Statement statement = delegate.createStatement();
      ProxyStatement result = trackStatement(ProxyFactory.getProxyStatement(this, statement));
      tailRecord.statementId = System.identityHashCode(result);

      if (!isFallbackMode())
         try {
            result.twinDelegate = twinDelegate.createStatement();
         } catch (SQLException e) {
            checkTwinException(e);
         }
      return result;
   }

   /** {@inheritDoc} */
   @Override
   public Statement createStatement(int resultSetType, int concurrency) throws SQLException
   {
      Statement statement = delegate.createStatement(resultSetType, concurrency);
      ProxyStatement result = trackStatement(ProxyFactory.getProxyStatement(this, statement));
      tailRecord.statementId = System.identityHashCode(result);

      if (!isFallbackMode())
         try {
            result.twinDelegate = twinDelegate.createStatement(resultSetType, concurrency);
         } catch (SQLException e) {
            checkTwinException(e);
         }
      return result;
   }

   /** {@inheritDoc} */
   @Override
   public Statement createStatement(int resultSetType, int concurrency, int holdability) throws SQLException
   {
      Statement statement = delegate.createStatement(resultSetType, concurrency, holdability);
      ProxyStatement result = trackStatement(ProxyFactory.getProxyStatement(this, statement));
      tailRecord.statementId = System.identityHashCode(result);

      if (!isFallbackMode())
         try {
            result.twinDelegate = twinDelegate.createStatement(resultSetType, concurrency, holdability);
         } catch (SQLException e) {
            checkTwinException(e);
         }
      return result;
   }

   /** {@inheritDoc} */
   @Override
   public CallableStatement prepareCall(String sql) throws SQLException
   {
      CallableStatement statement = delegate.prepareCall(sql);
      ProxyCallableStatement result = trackStatement(ProxyFactory.getProxyCallableStatement(this, statement));
      tailRecord.statementId = System.identityHashCode(result);

      if (!isFallbackMode())
         try {
            result.twinDelegate = twinDelegate.prepareCall(sql);
         } catch (SQLException e) {
            checkTwinException(e);
         }
      return result;
   }

   /** {@inheritDoc} */
   @Override
   public CallableStatement prepareCall(String sql, int resultSetType, int concurrency) throws SQLException
   {
      CallableStatement statement = delegate.prepareCall(sql, resultSetType, concurrency);
      ProxyCallableStatement result = trackStatement(ProxyFactory.getProxyCallableStatement(this, statement));
      tailRecord.statementId = System.identityHashCode(result);

      if (!isFallbackMode())
         try {
            result.twinDelegate = twinDelegate.prepareCall(sql, resultSetType, concurrency);
         } catch (SQLException e) {
            checkTwinException(e);
         }
      return result;
   }

   /** {@inheritDoc} */
   @Override
   public CallableStatement prepareCall(String sql, int resultSetType, int concurrency, int holdability) throws SQLException
   {
      CallableStatement statement = delegate.prepareCall(sql, resultSetType, concurrency, holdability);
      ProxyCallableStatement result = trackStatement(ProxyFactory.getProxyCallableStatement(this, statement));
      tailRecord.statementId = System.identityHashCode(result);

      if (!isFallbackMode())
         try {
            result.twinDelegate = twinDelegate.prepareCall(sql, resultSetType, concurrency, holdability);
         } catch (SQLException e) {
            checkTwinException(e);
         }
      return result;
   }

   /** {@inheritDoc} */
   @Override
   public PreparedStatement prepareStatement(String sql) throws SQLException
   {
      PreparedStatement statement = delegate.prepareStatement(sql);
      ProxyPreparedStatement result = trackStatement(ProxyFactory.getProxyPreparedStatement(this, statement));
      tailRecord.statementId = System.identityHashCode(result);

      if (!isFallbackMode())
         try {
            result.twinDelegate = twinDelegate.prepareStatement(sql);
         } catch (SQLException e) {
            checkTwinException(e);
         }
      return result;
   }

   /** {@inheritDoc} */
   @Override
   public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException
   {
      PreparedStatement statement = delegate.prepareStatement(sql, autoGeneratedKeys);
      ProxyPreparedStatement result = trackStatement(ProxyFactory.getProxyPreparedStatement(this, statement));
      tailRecord.statementId = System.identityHashCode(result);

      if (!isFallbackMode())
         try {
            result.twinDelegate = twinDelegate.prepareStatement(sql, autoGeneratedKeys);
         } catch (SQLException e) {
            checkTwinException(e);
         }
      return result;
   }

   /** {@inheritDoc} */
   @Override
   public PreparedStatement prepareStatement(String sql, int resultSetType, int concurrency) throws SQLException
   {
      PreparedStatement statement = delegate.prepareStatement(sql, resultSetType, concurrency);
      ProxyPreparedStatement result = trackStatement(ProxyFactory.getProxyPreparedStatement(this, statement));
      tailRecord.statementId = System.identityHashCode(result);

      if (!isFallbackMode())
         try {
            result.twinDelegate = twinDelegate.prepareStatement(sql, resultSetType, concurrency);
         } catch (SQLException e) {
            checkTwinException(e);
         }
      return result;
   }

   /** {@inheritDoc} */
   @Override
   public PreparedStatement prepareStatement(String sql, int resultSetType, int concurrency, int holdability) throws SQLException
   {
      PreparedStatement statement = delegate.prepareStatement(sql, resultSetType, concurrency, holdability);
      ProxyPreparedStatement result = trackStatement(ProxyFactory.getProxyPreparedStatement(this, statement));
      tailRecord.statementId = System.identityHashCode(result);

      if (!isFallbackMode())
         try {
            result.twinDelegate = twinDelegate.prepareStatement(sql, resultSetType, concurrency, holdability);
         } catch (SQLException e) {
            checkTwinException(e);
         }
      return result;
   }

   /** {@inheritDoc} */
   @Override
   public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException
   {
      PreparedStatement statement = delegate.prepareStatement(sql, columnIndexes);
      ProxyPreparedStatement result = trackStatement(ProxyFactory.getProxyPreparedStatement(this, statement));
      tailRecord.statementId = System.identityHashCode(result);

      if (!isFallbackMode())
         try {
            result.twinDelegate = twinDelegate.prepareStatement(sql, columnIndexes);
         } catch (SQLException e) {
            checkTwinException(e);
         }
      return result;
   }

   /** {@inheritDoc} */
   @Override
   public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException
   {
      PreparedStatement statement = delegate.prepareStatement(sql, columnNames);
      ProxyPreparedStatement result = trackStatement(ProxyFactory.getProxyPreparedStatement(this, statement));
      tailRecord.statementId = System.identityHashCode(result);

      if (!isFallbackMode())
         try {
            result.twinDelegate = twinDelegate.prepareStatement(sql, columnNames);
         } catch (SQLException e) {
            checkTwinException(e);
         }
      return result;
   }

   /** {@inheritDoc} */
   @Override
   public DatabaseMetaData getMetaData() throws SQLException
   {
      markCommitStateDirty();
      return delegate.getMetaData();
   }

   /** {@inheritDoc} */
   @Override
   @DontRecord
   public void commit() throws SQLException
   {
      delegate.commit();
      isCommitStateDirty = false;
      lastAccess = currentTime();

      if (!isFallbackMode()) {
         try {
            twinDelegate.commit();
            for (ProxyStatement openStatement : openStatements)
               openStatement.invocationQueue.clear();
            clearSuccessful();
         } catch (SQLException e) {
            for (ProxyStatement openStatement : openStatements)
               openStatement.drainQueue();
            invoked("commit ()V", Marshaller.emptyObjectArray);
            checkTwinException(e);
         }
      } else {
         for (ProxyStatement openStatement : openStatements)
            openStatement.drainQueue();
         invoked("commit ()V", Marshaller.emptyObjectArray);
         drainQueue();
      }
   }

   /** {@inheritDoc} */
   @Override
   @DontRecord
   public void rollback() throws SQLException
   {
      delegate.rollback();
      isCommitStateDirty = false;
      lastAccess = currentTime();

      for (ProxyStatement openStatement : openStatements)
         openStatement.invocationQueue.clear();
      clearSuccessful();

      if (!isFallbackMode())
         try {
            twinDelegate.rollback();
         } catch (SQLException e) {
            checkTwinException(e);
         }
   }

   /** {@inheritDoc} */
   @Override
   public void rollback(Savepoint savepoint) throws SQLException
   {
      delegate.rollback(savepoint);
      isCommitStateDirty = false;
      lastAccess = currentTime();

      if (!isFallbackMode())
         try {
            twinDelegate.rollback(savepoint);
            // todo ????
         } catch (SQLException e) {
            checkTwinException(e);
         }
   }

   /** {@inheritDoc} */
   @Override
   public void setAutoCommit(boolean autoCommit) throws SQLException
   {
      delegate.setAutoCommit(autoCommit);
      isAutoCommit = autoCommit;
      dirtyBits |= DIRTY_BIT_AUTOCOMMIT;

      if (!isFallbackMode())
         try {
            twinDelegate.setAutoCommit(autoCommit);
         } catch (SQLException e) {
            checkTwinException(e);
         }
   }

   /** {@inheritDoc} */
   @Override
   public void setReadOnly(boolean readOnly) throws SQLException
   {
      delegate.setReadOnly(readOnly);
      isReadOnly = readOnly;
      isCommitStateDirty = false;
      dirtyBits |= DIRTY_BIT_READONLY;

      if (!isFallbackMode())
         try {
            twinDelegate.setReadOnly(readOnly);
         } catch (SQLException e) {
            checkTwinException(e);
         }
   }

   /** {@inheritDoc} */
   @Override
   public void setTransactionIsolation(int level) throws SQLException
   {
      delegate.setTransactionIsolation(level);
      transactionIsolation = level;
      dirtyBits |= DIRTY_BIT_ISOLATION;

      if (!isFallbackMode())
         try {
            twinDelegate.setTransactionIsolation(level);
         } catch (SQLException e) {
            checkTwinException(e);
         }
   }

   /** {@inheritDoc} */
   @Override
   public void setCatalog(String catalog) throws SQLException
   {
      delegate.setCatalog(catalog);
      dbcatalog = catalog;
      dirtyBits |= DIRTY_BIT_CATALOG;

      if (!isFallbackMode())
         try {
            twinDelegate.setCatalog(catalog);
         } catch (SQLException e) {
            checkTwinException(e);
         }
   }

   @Override
   @DontRecord
   public void abort(Executor executor) throws SQLException {
      delegate.abort(executor);

//      if (!isFallbackMode())
//         try {
//            // todo ???
//            twinDelegate.abort(command -> {
//            });
//         } catch (SQLException e) {
//            checkTwinException(e);
//         }
   }

   /** {@inheritDoc} */
   @Override
   public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException
   {
      delegate.setNetworkTimeout(executor, milliseconds);
      networkTimeout = milliseconds;
      dirtyBits |= DIRTY_BIT_NETTIMEOUT;

//      if (!isFallbackMode())
//         try {
//            // todo ???
//            twinDelegate.setNetworkTimeout(command -> {
//            }, milliseconds);
//         } catch (SQLException e) {
//            checkTwinException(e);
//         }
   }

   /** {@inheritDoc} */
   @Override
   public void setSchema(String schema) throws SQLException
   {
      delegate.setSchema(schema);
      dbschema = schema;
      dirtyBits |= DIRTY_BIT_SCHEMA;

      if (!isFallbackMode())
         try {
            twinDelegate.setSchema(schema);
         } catch (SQLException e) {
            checkTwinException(e);
         }
   }

   /** {@inheritDoc} */
   @Override
   public final boolean isWrapperFor(Class<?> iface) throws SQLException
   {
      return iface.isInstance(delegate) || (delegate != null && delegate.isWrapperFor(iface));
   }

   /** {@inheritDoc} */
   @Override
   @SuppressWarnings("unchecked")
   public final <T> T unwrap(Class<T> iface) throws SQLException
   {
      if (iface.isInstance(delegate)) {
         return (T) delegate;
      }
      else if (delegate != null) {
          return delegate.unwrap(iface);
      }

      throw new SQLException("Wrapped connection is not an instance of " + iface);
   }

   // **********************************************************************
   //                         Private classes
   // **********************************************************************

   private static final class ClosedConnection
   {
      static final Connection CLOSED_CONNECTION = getClosedConnection();

      private static Connection getClosedConnection()
      {
         InvocationHandler handler = (proxy, method, args) -> {
            final String methodName = method.getName();
            if ("isClosed".equals(methodName)) {
               return Boolean.TRUE;
            }
            else if ("isValid".equals(methodName)) {
               return Boolean.FALSE;
            }
            if ("abort".equals(methodName)) {
               return Void.TYPE;
            }
            if ("close".equals(methodName)) {
               return Void.TYPE;
            }
            else if ("toString".equals(methodName)) {
               return ClosedConnection.class.getCanonicalName();
            }

            throw new SQLException("Connection is closed");
         };

         return (Connection) Proxy.newProxyInstance(Connection.class.getClassLoader(), new Class[] { Connection.class }, handler);
      }
   }
}
