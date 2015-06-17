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

package ru.programpark.hikari.proxy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.programpark.hikari.pool.HikariPool;
import ru.programpark.hikari.pool.LeakTask;
import ru.programpark.hikari.pool.PoolBagEntry;
import ru.programpark.hikari.util.FastList;
import ru.programpark.hikari.util.FSTHelper;

import java.sql.*;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;

import static java.lang.System.identityHashCode;

/**
 * This is the proxy class for java.sql.Connection.
 *
 * @author Brett Wooldridge
 */
public abstract class ConnectionProxy implements IHikariConnectionProxy
{

   protected static final Logger LOGGER = LoggerFactory.getLogger(ConnectionProxy.class);
   private static final Set<String> SQL_ERRORS;

   // static initializer
   static {
      SQL_ERRORS = new HashSet<String>();
      SQL_ERRORS.add("57P01"); // ADMIN SHUTDOWN
      SQL_ERRORS.add("57P02"); // CRASH SHUTDOWN
      SQL_ERRORS.add("57P03"); // CANNOT CONNECT NOW
      SQL_ERRORS.add("01002"); // SQL92 disconnect error
      SQL_ERRORS.add("JZ0C0"); // Sybase disconnect error
      SQL_ERRORS.add("JZ0C1"); // Sybase disconnect error
   }

   final ConcurrentLinkedQueue<Record> invocationQueue;
   private final LeakTask leakTask;
   private final HikariPool parentPool;
   private final PoolBagEntry bagEntry;
   private final FastList<GenericStatementProxy> openStatements;
   protected Connection delegate;
   protected Connection delegate2 = null;
   protected Record tailRecord;
   private PreparedStatement fallbackInsert;
   private long lastAccess;
   private boolean isCommitStateDirty;
   private boolean isConnectionStateDirty;
   private boolean isAutoCommitDirty;
   private boolean isCatalogDirty;
   private boolean isReadOnlyDirty;
   private boolean isTransactionIsolationDirty;

   protected ConnectionProxy(final HikariPool pool, final PoolBagEntry bagEntry, final LeakTask leakTask)
   {
      this.delegate = bagEntry.connection;
      // already under lock
      if (pool.poolState == HikariPool.POOL_RUNNING)
         try {
            this.delegate2 = pool.getConfiguration().getDataSource2().getConnection();
            pool.setupConnection(this.delegate2);
         }
         catch (SQLException e) {
            pool.poolState = HikariPool.POOL_FALLBACK;
            this.delegate2 = null;
            LOGGER.warn("Error getting slave connection");
         }
      this.invocationQueue = new ConcurrentLinkedQueue<Record>();

      this.parentPool = pool;
      this.bagEntry = bagEntry;
      this.leakTask = leakTask;
      this.lastAccess = bagEntry.lastAccess;

      this.openStatements = new FastList<GenericStatementProxy>(Statement.class, 16);
   }

   @Override
   public String toString()
   {
      return String.format("%s(%s) wrapping %s", this.getClass().getSimpleName(), System.identityHashCode(this), delegate);
   }

   // ***********************************************************************
   //                      IHikariConnectionProxy methods
   // ***********************************************************************

   /**
    * {@inheritDoc}
    */
   @Override
   public final PoolBagEntry getPoolBagEntry()
   {
      return bagEntry;
   }

   /**
    * {@inheritDoc}
    */
//	@Override
   public final SQLException checkException(final SQLException sqle)
   {
      String sqlState = sqle.getSQLState();
      if (sqlState != null) {
         boolean isForceClose = sqlState.startsWith("08") | SQL_ERRORS.contains(sqlState);
         if (isForceClose) {
            bagEntry.evicted = true;
            LOGGER.warn("Connection {} ({}) marked as broken because of SQLSTATE({}), ErrorCode({}).", delegate.toString(),
                    parentPool.toString(), sqlState, sqle.getErrorCode(), sqle);
         }
         else if (sqle.getNextException() != null && sqle != sqle.getNextException()) {
            checkException(sqle.getNextException());
         }
      }
      return sqle;
   }

   public final void checkException2(final SQLException sqle) throws SQLException
   {
      if (LOGGER.isTraceEnabled())
         LOGGER.trace(getClass().getName() + ".checkException2", sqle);
      parentPool.fallback();
      if (delegate2 != null) {
         try {
            delegate2.close();
         }
         catch (SQLException e) {
            e.printStackTrace(); // todo ???
         }
         delegate2 = null;
      }
      drainQueue();
   }

   protected boolean isFallbackMode()
   {
      return delegate2 == null;
   }

   private final void drainQueue() throws SQLException
   {
      try {
         if (fallbackInsert == null)
            fallbackInsert = delegate.prepareStatement("INSERT INTO invocation_queue (connection_id, statement_id, class, method_name, args) VALUES (?, ?, ?, ?, ?)");

         for (GenericStatementProxy openStatement : openStatements)
            openStatement.drainQueue();

         int i = 0;
         Iterator<Record> iterator = invocationQueue.iterator();
         while (iterator.hasNext()) {
            Record record = iterator.next();

            fallbackInsert.setInt(1, record.conncetionId);
            fallbackInsert.setInt(2, record.statementId);
            fallbackInsert.setInt(3, record.clazz.getName().hashCode());
            fallbackInsert.setString(4, record.methodName);
            if (record.args.length == 0)
               fallbackInsert.setNull(5, Types.NULL);
            else
               fallbackInsert.setBytes(5, FSTHelper.FST.asByteArray(record.args));

            fallbackInsert.addBatch();
            iterator.remove();
            i++;
         }
         if (i > 0)
            fallbackInsert.executeBatch();
         if (!delegate.getAutoCommit())
            delegate.commit();
      }
      catch (SQLException e) {
         throw checkException(e);
      }
   }

   private final int getConnectionId()
   {
      return System.identityHashCode(this);
   }

   private final int getStatementId()
   {
      return 0;
   }

   private final void clearSuccessful()
   {
      Iterator<Record> iterator = invocationQueue.iterator();
      while (iterator.hasNext())
         if (iterator.next().statementId != 0)
            iterator.remove();
   }

   protected final void invoked(String methodName, Object... args)
   {
      invocationQueue.add(tailRecord = new Record(getConnectionId(), getStatementId(), this.getClass(), methodName, args));
      if (GenericStatementProxy.LOGGER.isTraceEnabled())
         GenericStatementProxy.LOGGER.trace(tailRecord.toString());
   }

   /**
    * {@inheritDoc}
    */
//	@Override
   public final void untrackStatement(final GenericStatementProxy statement)
   {
      openStatements.remove(statement);
   }

   /**
    * {@inheritDoc}
    */
//	@Override
   public final void markCommitStateDirty()
   {
      isCommitStateDirty = true;
      lastAccess = System.currentTimeMillis();
   }

   // ***********************************************************************
   //                        Internal methods
   // ***********************************************************************

   private final <T extends GenericStatementProxy> T trackStatement(final T statement)
   {
      lastAccess = System.currentTimeMillis();
      openStatements.add(statement);

      return statement;
   }

   private final void resetConnectionState() throws SQLException
   {
      if (isReadOnlyDirty) {
         delegate.setReadOnly(parentPool.isReadOnly);
         if (!isFallbackMode())
            try {
               delegate2.setReadOnly(parentPool.isReadOnly);
            }
            catch (SQLException e) {
               checkException2(e);
            }
      }

      if (isAutoCommitDirty) {
         delegate.setAutoCommit(parentPool.isAutoCommit);
         if (!isFallbackMode())
            try {
               delegate2.setAutoCommit(parentPool.isAutoCommit);
            }
            catch (SQLException e) {
               checkException2(e);
            }
      }

      if (isTransactionIsolationDirty) {
         delegate.setTransactionIsolation(parentPool.transactionIsolation);
         if (!isFallbackMode())
            try {
               delegate2.setTransactionIsolation(parentPool.transactionIsolation);
            }
            catch (SQLException e) {
               checkException2(e);
            }
      }

      if (isCatalogDirty && parentPool.catalog != null) {
         delegate.setCatalog(parentPool.catalog);
         if (!isFallbackMode())
            try {
               delegate2.setCatalog(parentPool.catalog);
            }
            catch (SQLException e) {
               checkException2(e);
            }
      }

      Iterator<Record> iterator = invocationQueue.iterator();
      while (iterator.hasNext())
         iterator.remove();
   }

   // **********************************************************************
   //                   "Overridden" java.sql.Connection Methods
   // **********************************************************************

   /**
    * {@inheritDoc}
    */
   @Override
   public void close() throws SQLException
   {
      if (delegate != ClosedConnection.CLOSED_CONNECTION) {
         leakTask.cancel();

         for (GenericStatementProxy openStatement : openStatements) {
            try {
               openStatement.delegate.close();
               if (openStatement.delegate2 != null)
                  try {
                     openStatement.delegate2.close();
                  }
                  catch (SQLException e) {
                     e.printStackTrace();
                     // todo ???
                  }
            }
            catch (SQLException e) {
               checkException(e);
            }
         }

         try {
            if (isCommitStateDirty && !delegate.getAutoCommit()) {
               delegate.rollback();
               if (!isFallbackMode())
                  try {
                     delegate2.rollback();
                  }
                  catch (SQLException e) {
                     checkException2(e);
                  }
            }
            else if (isFallbackMode())
               drainQueue();

            if (isConnectionStateDirty) {
               resetConnectionState();
            }

            delegate.clearWarnings();
            if (!isFallbackMode())
               try {
                  delegate2.clearWarnings();
               }
               catch (SQLException e) {
                  checkException2(e);
               }

         }

         catch (SQLException e) {
            // when connections are aborted, exceptions are often thrown that should not reach the application
            if (!bagEntry.aborted) {
               throw checkException(e);
            }
         }
         finally {
            delegate = ClosedConnection.CLOSED_CONNECTION;
            delegate2 = null;
            bagEntry.lastAccess = lastAccess;
            parentPool.releaseConnection(bagEntry);
         }
      }
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public boolean isClosed() throws SQLException
   {
      return (delegate == ClosedConnection.CLOSED_CONNECTION);
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public Statement createStatement() throws SQLException
   {
      Statement statement = delegate.createStatement();
      StatementProxy result = trackStatement(ProxyFactory.getProxyStatement(this, statement));
      tailRecord.statementId = identityHashCode(result);
      if (!isFallbackMode())
         try {
            result.delegate2 = delegate2.createStatement();
         }
         catch (SQLException e) {
            checkException2(e);
         }
      return result;
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public Statement createStatement(int resultSetType, int concurrency) throws SQLException
   {
      Statement statement = delegate.createStatement(resultSetType, concurrency);
      StatementProxy result = trackStatement(ProxyFactory.getProxyStatement(this, statement));
      tailRecord.statementId = identityHashCode(result);
      if (!isFallbackMode())
         try {
            result.delegate2 = delegate2.createStatement(resultSetType, concurrency);
         }
         catch (SQLException e) {
            checkException2(e);
         }
      return result;
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public Statement createStatement(int resultSetType, int concurrency, int holdability) throws SQLException
   {
      Statement statement = delegate.createStatement(resultSetType, concurrency, holdability);
      StatementProxy result = trackStatement(ProxyFactory.getProxyStatement(this, statement));
      tailRecord.statementId = identityHashCode(result);
      if (!isFallbackMode())
         try {
            result.delegate2 = delegate2.createStatement(resultSetType, concurrency, holdability);
         }
         catch (SQLException e) {
            checkException2(e);
         }
      return result;
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public CallableStatement prepareCall(String sql) throws SQLException
   {
      CallableStatement statement = delegate.prepareCall(sql);
      CallableStatementProxy result = trackStatement(ProxyFactory.getProxyCallableStatement(this, statement));
      tailRecord.statementId = identityHashCode(result);
      if (!isFallbackMode())
         try {
            result.delegate2 = delegate2.prepareCall(sql);
         }
         catch (SQLException e) {
            checkException2(e);
         }
      return result;
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public CallableStatement prepareCall(String sql, int resultSetType, int concurrency) throws SQLException
   {
      CallableStatement statement = delegate.prepareCall(sql, resultSetType, concurrency);
      CallableStatementProxy result = trackStatement(ProxyFactory.getProxyCallableStatement(this, statement));
      tailRecord.statementId = identityHashCode(result);
      if (!isFallbackMode())
         try {
            result.delegate2 = delegate2.prepareCall(sql, resultSetType, concurrency);
         }
         catch (SQLException e) {
            checkException2(e);
         }
      return result;
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public CallableStatement prepareCall(String sql, int resultSetType, int concurrency, int holdability) throws SQLException
   {
      CallableStatement statement = delegate.prepareCall(sql, resultSetType, concurrency, holdability);
      CallableStatementProxy result = trackStatement(ProxyFactory.getProxyCallableStatement(this, statement));
      tailRecord.statementId = identityHashCode(result);
      if (!isFallbackMode())
         try {
            result.delegate2 = delegate2.prepareCall(sql, resultSetType, concurrency, holdability);
         }
         catch (SQLException e) {
            checkException2(e);
         }
      return result;
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public PreparedStatement prepareStatement(String sql) throws SQLException
   {
      PreparedStatement statement = delegate.prepareStatement(sql);
      PreparedStatementProxy result = trackStatement(ProxyFactory.getProxyPreparedStatement(this, statement));
      tailRecord.statementId = identityHashCode(result);
      if (!isFallbackMode())
         try {
            result.delegate2 = delegate2.prepareStatement(sql);
         }
         catch (SQLException e) {
            checkException2(e);
         }
      return result;
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException
   {
      PreparedStatement statement = delegate.prepareStatement(sql, autoGeneratedKeys);
      PreparedStatementProxy result = trackStatement(ProxyFactory.getProxyPreparedStatement(this, statement));
      tailRecord.statementId = identityHashCode(result);
      if (!isFallbackMode())
         try {
            result.delegate2 = delegate2.prepareStatement(sql, autoGeneratedKeys);
         }
         catch (SQLException e) {
            checkException2(e);
         }
      return result;
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public PreparedStatement prepareStatement(String sql, int resultSetType, int concurrency) throws SQLException
   {
      PreparedStatement statement = delegate.prepareStatement(sql, resultSetType, concurrency);
      PreparedStatementProxy result = trackStatement(ProxyFactory.getProxyPreparedStatement(this, statement));
      tailRecord.statementId = identityHashCode(result);
      if (!isFallbackMode())
         try {
            result.delegate2 = delegate2.prepareStatement(sql, resultSetType, concurrency);
         }
         catch (SQLException e) {
            checkException2(e);
         }
      return result;
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public PreparedStatement prepareStatement(String sql, int resultSetType, int concurrency, int holdability) throws SQLException
   {
      PreparedStatement statement = delegate.prepareStatement(sql, resultSetType, concurrency, holdability);
      PreparedStatementProxy result = trackStatement(ProxyFactory.getProxyPreparedStatement(this, statement));
      tailRecord.statementId = identityHashCode(result);
      if (!isFallbackMode())
         try {
            result.delegate2 = delegate2.prepareStatement(sql, resultSetType, concurrency, holdability);
         }
         catch (SQLException e) {
            checkException2(e);
         }
      return result;
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException
   {
      PreparedStatement statement = delegate.prepareStatement(sql, columnIndexes);
      PreparedStatementProxy result = trackStatement(ProxyFactory.getProxyPreparedStatement(this, statement));
      tailRecord.statementId = identityHashCode(result);
      if (!isFallbackMode())
         try {
            result.delegate2 = delegate2.prepareStatement(sql, columnIndexes);
         }
         catch (SQLException e) {
            checkException2(e);
         }
      return result;
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException
   {
      PreparedStatement statement = delegate.prepareStatement(sql, columnNames);
      PreparedStatementProxy result = trackStatement(ProxyFactory.getProxyPreparedStatement(this, statement));
      tailRecord.statementId = identityHashCode(result);
      if (!isFallbackMode())
         try {
            result.delegate2 = delegate2.prepareStatement(sql, columnNames);
         }
         catch (SQLException e) {
            checkException2(e);
         }
      return result;
   }

   /**
    * {@inheritDoc}
    */
   @Override
   @DontRecord
   public void commit() throws SQLException
   {
      delegate.commit();
      isCommitStateDirty = false;
      if (!isFallbackMode()) {
         try {
            delegate2.commit();
            for (GenericStatementProxy openStatement : openStatements)
               openStatement.invocationQueue.clear();
            clearSuccessful();
         }
         catch (SQLException e) {
            for (GenericStatementProxy openStatement : openStatements)
               openStatement.drainQueue();
            invoked("commit");
            checkException2(e);
         }
      }
      else {
         for (GenericStatementProxy openStatement : openStatements)
            openStatement.drainQueue();
         invoked("commit");
         drainQueue();
      }
   }

   /**
    * {@inheritDoc}
    */
   @Override
   @DontRecord
   public void rollback() throws SQLException
   {
      delegate.rollback();
      isCommitStateDirty = false;

      for (GenericStatementProxy openStatement : openStatements)
         openStatement.invocationQueue.clear();
      clearSuccessful();

      if (!isFallbackMode())
         try {
            delegate2.rollback();
         }
         catch (SQLException e) {
            checkException2(e);
         }
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public void rollback(Savepoint savepoint) throws SQLException
   {
      delegate.rollback(savepoint);
      isCommitStateDirty = false;
      if (!isFallbackMode())
         try {
            delegate2.rollback(savepoint);
            // todo ????
         }
         catch (SQLException e) {
            checkException2(e);
         }
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public void setAutoCommit(boolean autoCommit) throws SQLException
   {
      delegate.setAutoCommit(autoCommit);
      isConnectionStateDirty = true;
      isAutoCommitDirty = (autoCommit != parentPool.isAutoCommit);
      if (!isFallbackMode())
         try {
            delegate2.setAutoCommit(autoCommit);
         }
         catch (SQLException e) {
            checkException2(e);
         }
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public void setReadOnly(boolean readOnly) throws SQLException
   {
      delegate.setReadOnly(readOnly);
      isConnectionStateDirty = true;
      isReadOnlyDirty = (readOnly != parentPool.isReadOnly);
      if (!isFallbackMode())
         try {
            delegate2.setReadOnly(readOnly);
         }
         catch (SQLException e) {
            checkException2(e);
         }
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public void setTransactionIsolation(int level) throws SQLException
   {
      delegate.setTransactionIsolation(level);
      isConnectionStateDirty = true;
      isTransactionIsolationDirty = (level != parentPool.transactionIsolation);
      if (!isFallbackMode())
         try {
            delegate2.setTransactionIsolation(level);
         }
         catch (SQLException e) {
            checkException2(e);
         }
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public void setCatalog(String catalog) throws SQLException
   {
      delegate.setCatalog(catalog);
      isConnectionStateDirty = true;
      isCatalogDirty = (catalog != null && !catalog.equals(parentPool.catalog)) || (catalog == null && parentPool.catalog != null);
      if (!isFallbackMode())
         try {
            delegate2.setCatalog(catalog);
         }
         catch (SQLException e) {
            checkException2(e);
         }
   }

   @Override
   @DontRecord
   public void abort(Executor executor) throws SQLException
   {
      delegate.abort(executor);
//		if(!isFallbackMode())
//			try {
//				// todo ???
//				delegate2.abort(command -> {});
//			} catch(SQLException e) {
//				checkException2(e);
//			}
   }

   @Override
   @DontRecord
   public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException
   {
      delegate.setNetworkTimeout(executor, milliseconds);
//		if(!isFallbackMode())
//			try {
//				// todo ???
//				delegate2.setNetworkTimeout(command -> {}, milliseconds);
//			} catch(SQLException e) {
//				checkException2(e);
//			}
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public final boolean isWrapperFor(Class<?> iface) throws SQLException
   {
      return iface.isInstance(delegate) || (delegate instanceof Wrapper && delegate.isWrapperFor(iface));
   }

   /**
    * {@inheritDoc}
    */
   @Override
   @SuppressWarnings("unchecked")
   public final <T> T unwrap(Class<T> iface) throws SQLException
   {
      if (iface.isInstance(delegate)) {
         return (T) delegate;
      }
      else if (delegate instanceof Wrapper) {
         return (T) delegate.unwrap(iface);
      }

      throw new SQLException("Wrapped connection is not an instance of " + iface);
   }

}
