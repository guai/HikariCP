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

package com.zaxxer.hikari.pool;

import com.zaxxer.hikari.util.CacheByteSource;
import com.zaxxer.hikari.util.CacheCharSource;
import lombok.SneakyThrows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.io.Reader;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * This is the proxy class for java.sql.PreparedStatement.
 *
 * @author Brett Wooldridge
 */
public abstract class ProxyPreparedStatement<T extends PreparedStatement> extends ProxyStatement<T> implements PreparedStatement
{
   private static final Logger LOGGER = LoggerFactory.getLogger(ProxyPreparedStatement.class);

   ProxyPreparedStatement(ProxyConnection connection, T statement)
   {
      super(connection, statement);
   }

   @Override
   public char getClassId() {
      return 'P';
   }

   // **********************************************************************
   //              Overridden java.sql.PreparedStatement Methods
   // **********************************************************************

   /** {@inheritDoc} */
   @Override
   public boolean execute() throws SQLException
   {
      connection.markCommitStateDirty();
      boolean result = delegate.execute();

      if (!isFallbackMode()) try {
         twinDelegate.execute();
      } catch (SQLException e) {
         checkTwinException(e);
      }
      return result;
   }

   /** {@inheritDoc} */
   @Override
   public ResultSet executeQuery() throws SQLException
   {
      connection.markCommitStateDirty();
      ResultSet resultSet = delegate.executeQuery();

      if (!isFallbackMode())
         try {
            twinDelegate.executeQuery();
         } catch (SQLException e) {
            checkTwinException(e);
         }
      return ProxyFactory.getProxyResultSet(connection, this, resultSet);
   }

   /** {@inheritDoc} */
   @Override
   public int executeUpdate() throws SQLException
   {
      connection.markCommitStateDirty();
      int result = delegate.executeUpdate();

      if (!isFallbackMode()) try {
         twinDelegate.executeUpdate();
      } catch (SQLException e) {
         checkTwinException(e);
      }
      return result;
   }

   /** {@inheritDoc} */
   @Override
   public long executeLargeUpdate() throws SQLException
   {
      connection.markCommitStateDirty();
      long result = delegate.executeLargeUpdate();

      if (!isFallbackMode())
         try {
            twinDelegate.executeLargeUpdate();
         } catch (SQLException e) {
            checkTwinException(e);
         }
      return result;
   }


   @Override
   @SneakyThrows
   public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
      CacheByteSource re = new CacheByteSource(x, length);
      tailRecord.args[1] = re;
      delegate.setAsciiStream(parameterIndex, re.openStream(), length);

      if (!isFallbackMode())
         try {
            twinDelegate.setAsciiStream(parameterIndex, re.openStream(), length);
         } catch (SQLException e) {
            checkTwinException(e);
         }
   }

   @Override
   @SneakyThrows
   public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
      CacheByteSource re = new CacheByteSource(x, length);
      tailRecord.args[1] = re;
      delegate.setBinaryStream(parameterIndex, re.openStream(), length);

      if (!isFallbackMode())
         try {
            twinDelegate.setBinaryStream(parameterIndex, re.openStream(), length);
         } catch (SQLException e) {
            checkTwinException(e);
         }
   }

   @Override
   @SneakyThrows
   public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
      CacheCharSource re = new CacheCharSource(reader, length);
      tailRecord.args[1] = re;
      delegate.setCharacterStream(parameterIndex, re.openStream(), length);

      if (!isFallbackMode())
         try {
            twinDelegate.setCharacterStream(parameterIndex, re.openStream(), length);
         } catch (SQLException e) {
            checkTwinException(e);
         }
   }

   @Override
   @SneakyThrows
   public void setNCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
      CacheCharSource re = new CacheCharSource(reader, (int) length);
      tailRecord.args[1] = re;
      delegate.setNCharacterStream(parameterIndex, re.openStream(), length);

      if (!isFallbackMode())
         try {
            twinDelegate.setNCharacterStream(parameterIndex, re.openStream(), length);
         } catch (SQLException e) {
            checkTwinException(e);
         }
   }

   @Override
   public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
      setAsciiStream(parameterIndex, x, (int) length);
   }

   @Override
   public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
      setBinaryStream(parameterIndex, x, (int) length);
   }

   @Override
   public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
      setCharacterStream(parameterIndex, reader, (int) length);
   }

   @Override
   @SneakyThrows
   public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
      CacheByteSource re = new CacheByteSource(x);
      tailRecord.args[1] = re;
      delegate.setAsciiStream(parameterIndex, re.openStream());

      if (!isFallbackMode())
         try {
            twinDelegate.setAsciiStream(parameterIndex, re.openStream());
         } catch (SQLException e) {
            checkTwinException(e);
         }
   }

   @Override
   @SneakyThrows
   public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
      CacheByteSource re = new CacheByteSource(x);
      tailRecord.args[1] = re;
      delegate.setBinaryStream(parameterIndex, re.openStream());

      if (!isFallbackMode())
         try {
            twinDelegate.setBinaryStream(parameterIndex, re.openStream());
         } catch (SQLException e) {
            checkTwinException(e);
         }
   }

   @Override
   @SneakyThrows
   public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
      CacheCharSource re = new CacheCharSource(reader);
      tailRecord.args[1] = re;
      delegate.setCharacterStream(parameterIndex, re.openStream());

      if (!isFallbackMode())
         try {
            twinDelegate.setCharacterStream(parameterIndex, re.openStream());
         } catch (SQLException e) {
            checkTwinException(e);
         }
   }

   @Override
   @SneakyThrows
   public void setNCharacterStream(int parameterIndex, Reader reader) throws SQLException {
      CacheCharSource re = new CacheCharSource(reader);
      tailRecord.args[1] = re;
      delegate.setNCharacterStream(parameterIndex, re.openStream());

      if (!isFallbackMode())
         try {
            twinDelegate.setNCharacterStream(parameterIndex, re.openStream());
         } catch (SQLException e) {
            checkTwinException(e);
         }
   }
}
