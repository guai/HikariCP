package com.zaxxer.hikari.pool;

import com.zaxxer.hikari.util.FastList;

import java.sql.*;

public class ProxyFactory implements IProxyFactory<ProxyConnection, ProxyStatement> {

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
	@Override
	public ProxyConnection getProxyConnection(final PoolEntry poolEntry, final Connection connection, final FastList<Statement> openStatements, final ProxyLeakTask leakTask, final long now, final boolean isReadOnly, final boolean isAutoCommit)
	{
		// Body is replaced (injected) by JavassistProxyFactory
		throw new IllegalStateException("You need to run the CLI build and you need target/classes in your classpath to run.");
	}

	@Override
	public Statement getProxyStatement(final ProxyConnection connection, final Statement statement)
	{
		// Body is replaced (injected) by JavassistProxyFactory
		throw new IllegalStateException("You need to run the CLI build and you need target/classes in your classpath to run.");
	}

	@Override
	public CallableStatement getProxyCallableStatement(final ProxyConnection connection, final CallableStatement statement)
	{
		// Body is replaced (injected) by JavassistProxyFactory
		throw new IllegalStateException("You need to run the CLI build and you need target/classes in your classpath to run.");
	}

	@Override
	public PreparedStatement getProxyPreparedStatement(final ProxyConnection connection, final PreparedStatement statement)
	{
		// Body is replaced (injected) by JavassistProxyFactory
		throw new IllegalStateException("You need to run the CLI build and you need target/classes in your classpath to run.");
	}

	@Override
	public ResultSet getProxyResultSet(final ProxyConnection connection, final ProxyStatement statement, final ResultSet resultSet)
	{
		// Body is replaced (injected) by JavassistProxyFactory
		throw new IllegalStateException("You need to run the CLI build and you need target/classes in your classpath to run.");
	}
}