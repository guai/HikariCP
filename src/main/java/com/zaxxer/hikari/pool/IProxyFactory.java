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

import com.zaxxer.hikari.util.FastList;

import java.sql.*;

/**
 * A factory class that produces proxies around instances of the standard
 * JDBC interfaces.
 *
 * @author Brett Wooldridge
 */
public interface IProxyFactory <ProxyConnection extends Connection, ProxyStatement extends Statement>
{
   /**
    * Create a proxy for the specified {@link Connection} instance.
    * @param poolEntry
    * @param connection
    * @param openStatements
    * @param leakTask
    * @param now
    * @param isReadOnly
    * @param isAutoCommit
    * @return a proxy that wraps the specified {@link Connection}
    */
   ProxyConnection getProxyConnection(final PoolEntry poolEntry, final Connection connection, final FastList<Statement> openStatements, final ProxyLeakTask leakTask, final long now, final boolean isReadOnly, final boolean isAutoCommit);
   Statement getProxyStatement(final ProxyConnection connection, final Statement statement);
   CallableStatement getProxyCallableStatement(final ProxyConnection connection, final CallableStatement statement);
   PreparedStatement getProxyPreparedStatement(final ProxyConnection connection, final PreparedStatement statement);
   ResultSet getProxyResultSet(final ProxyConnection connection, final ProxyStatement statement, final ResultSet resultSet);
}
