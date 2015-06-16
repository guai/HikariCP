package ru.programpark.hikari;

import org.junit.Assert;
import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;

public class IsolationTest
{
    @Test
    public void testIsolation() throws SQLException
    {
        HikariDataSource ds = new HikariDataSource();
        ds.setMinimumIdle(1);
        ds.setMaximumPoolSize(1);
        ds.setIsolateInternalQueries(true);
        ds.setDataSourceClassName("ru.programpark.hikari.mocks.StubDataSource");

        try
        {
            Connection connection = ds.getConnection();
            connection.close();

            Connection connection2 = ds.getConnection();
            connection2.close();

            Assert.assertNotSame(connection, connection2);
            Assert.assertSame(connection.unwrap(Connection.class), connection2.unwrap(Connection.class));
        }
        finally
        {
            ds.close();
        }
    }

    @Test
    public void testNonIsolation() throws SQLException
    {
        HikariDataSource ds = new HikariDataSource();
        ds.setMinimumIdle(1);
        ds.setMaximumPoolSize(1);
        ds.setIsolateInternalQueries(false);
        ds.setDataSourceClassName("ru.programpark.hikari.mocks.StubDataSource");

        try
        {
            Connection connection = ds.getConnection();
            connection.close();

            Connection connection2 = ds.getConnection();
            connection2.close();

            Assert.assertSame(connection.unwrap(Connection.class), connection2.unwrap(Connection.class));
        }
        finally
        {
            ds.close();
        }
    }
}
