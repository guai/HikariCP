/*
 * Copyright (C) 2013 Brett Wooldridge
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.Queue;

/**
 * This is the proxy class for java.sql.Statement.
 *
 * @author Brett Wooldridge
 */
abstract class GenericStatementProxy<T extends Statement> implements Statement
{

   protected static final Logger LOGGER = LoggerFactory.getLogger(GenericStatementProxy.class);
   protected final ConnectionProxy connection;
   final ArrayDeque<Record> invocationQueue;
   protected T delegate;
   protected T delegate2;
   protected Record tailRecord;

   private boolean isClosed;

   protected GenericStatementProxy(ConnectionProxy connection, T statement)
   {
      this.connection = connection;
      this.delegate = statement;
      this.invocationQueue = new ArrayDeque<Record>(5);
   }

   protected final SQLException checkException(SQLException e)
   {
      return connection.checkException(e);
   }

   protected final void checkException2(SQLException sqle) throws SQLException
   {
      if (LOGGER.isTraceEnabled())
         LOGGER.trace(getClass().getName() + ".checkException2", sqle);
      if (delegate2 != null) {
         try {
            delegate2.close();
         }
         catch (SQLException e) {
            e.printStackTrace(); // todo ???
         }
         delegate2 = null;
      }
      connection.checkException2(sqle);
   }

   protected final boolean isFallbackMode()
   {
      return delegate2 == null;
   }

   final void drainQueue()
   {
      if (invocationQueue.size() == 0) return;
      Queue<Record> connectionQueue = connection.invocationQueue;
      Iterator<Record> iterator = invocationQueue.iterator();
      while (iterator.hasNext()) {
         Record record = iterator.next();
         connectionQueue.add(record);
         iterator.remove();
      }
   }

   private final int getConnectionId()
   {
      return System.identityHashCode(connection);
   }

   private final int getStatementId()
   {
      return System.identityHashCode(this);
   }

   protected final void invoked(String methodName, Object... args)
   {
      invocationQueue.add(tailRecord = new Record(getConnectionId(), getStatementId(), this.getClass(), methodName, args));
      if (LOGGER.isTraceEnabled())
         LOGGER.trace(tailRecord.toString());
   }

   // **********************************************************************
   //                 Overridden java.sql.Statement Methods
   // **********************************************************************

   /**
    * {@inheritDoc}
    */
   @Override
   public void close() throws SQLException
   {
      if (isClosed) {
         return;
      }

      isClosed = true;
      connection.untrackStatement(this);

      try {
         delegate.close();
         if (delegate2 != null)
            try {
               delegate2.close();
            }
            catch (SQLException e) {
               e.printStackTrace(); // todo ???
            }

         if (isFallbackMode()) drainQueue();
      }
      catch (SQLException e) {
         throw checkException(e);
      }
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public Connection getConnection() throws SQLException
   {
      return connection;
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public boolean execute(String sql) throws SQLException
   {
      connection.markCommitStateDirty();
      boolean result = delegate.execute(sql);
      if (!isFallbackMode())
         try {
            delegate2.execute(sql);
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
   public boolean execute(String sql, int autoGeneratedKeys) throws SQLException
   {
      connection.markCommitStateDirty();
      boolean result = delegate.execute(sql, autoGeneratedKeys);
      if (!isFallbackMode())
         try {
            delegate2.execute(sql, autoGeneratedKeys);
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
   public ResultSet executeQuery(String sql) throws SQLException
   {
      connection.markCommitStateDirty();
      ResultSet resultSet = delegate.executeQuery(sql);
      // todo check if always 'select'
      return ProxyFactory.getProxyResultSet(connection, resultSet);
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public int executeUpdate(String sql) throws SQLException
   {
      connection.markCommitStateDirty();
      int result = delegate.executeUpdate(sql);
      if (!isFallbackMode())
         try {
            delegate2.executeUpdate(sql);
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
   public int[] executeBatch() throws SQLException
   {
      connection.markCommitStateDirty();
      int[] result = delegate.executeBatch();
      try {
         if (!isFallbackMode()) delegate2.executeBatch();
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
   public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException
   {
      connection.markCommitStateDirty();
      int result = delegate.executeUpdate(sql, autoGeneratedKeys);
      if (!isFallbackMode())
         try {
            delegate2.executeUpdate(sql, autoGeneratedKeys);
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
   public int executeUpdate(String sql, int[] columnIndexes) throws SQLException
   {
      connection.markCommitStateDirty();
      int result = delegate.executeUpdate(sql, columnIndexes);
      if (!isFallbackMode())
         try {
            delegate2.executeUpdate(sql, columnIndexes);
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
   public int executeUpdate(String sql, String[] columnNames) throws SQLException
   {
      connection.markCommitStateDirty();
      int result = delegate.executeUpdate(sql, columnNames);
      if (!isFallbackMode())
         try {
            delegate2.executeUpdate(sql, columnNames);
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
   public boolean execute(String sql, int[] columnIndexes) throws SQLException
   {
      connection.markCommitStateDirty();
      boolean result = delegate.execute(sql, columnIndexes);
      if (!isFallbackMode())
         try {
            delegate2.execute(sql, columnIndexes);
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
   public boolean execute(String sql, String[] columnNames) throws SQLException
   {
      connection.markCommitStateDirty();
      boolean result = delegate.execute(sql, columnNames);
      if (!isFallbackMode())
         try {
            delegate2.execute(sql, columnNames);
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
   public long[] executeLargeBatch() throws SQLException
   {
      connection.markCommitStateDirty();
      long[] result = delegate.executeLargeBatch();
      try {
         delegate.executeLargeBatch();
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
   public long executeLargeUpdate(String sql) throws SQLException
   {
      connection.markCommitStateDirty();
      long result = delegate.executeLargeUpdate(sql);
      if (!isFallbackMode())
         try {
            delegate2.executeLargeUpdate(sql);
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
   public long executeLargeUpdate(String sql, int autoGeneratedKeys) throws SQLException
   {
      connection.markCommitStateDirty();
      long result = delegate.executeLargeUpdate(sql, autoGeneratedKeys);
      if (!isFallbackMode())
         try {
            delegate2.executeLargeUpdate(sql, autoGeneratedKeys);
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
   public long executeLargeUpdate(String sql, int[] columnIndexes) throws SQLException
   {
      connection.markCommitStateDirty();
      long result = delegate.executeLargeUpdate(sql, columnIndexes);
      if (!isFallbackMode())
         try {
            delegate2.executeLargeUpdate(sql, columnIndexes);
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
   public long executeLargeUpdate(String sql, String[] columnNames) throws SQLException
   {
      connection.markCommitStateDirty();
      long result = delegate.executeLargeUpdate(sql, columnNames);
      if (!isFallbackMode())
         try {
            delegate2.executeLargeUpdate(sql, columnNames);
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
   public ResultSet getResultSet() throws SQLException
   {
      final ResultSet resultSet = delegate.getResultSet();
      //todo check if always 'select'
      if (resultSet != null) {
         return ProxyFactory.getProxyResultSet(connection, resultSet);
      }
      return null;
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

      throw new SQLException("Wrapped statement is not an instance of " + iface);
   }

}
