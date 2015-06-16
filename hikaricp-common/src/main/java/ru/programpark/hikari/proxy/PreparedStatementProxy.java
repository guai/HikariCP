package ru.programpark.hikari.proxy;

import java.sql.PreparedStatement;

abstract public class PreparedStatementProxy extends GenericPreparedStatementProxy<PreparedStatement>
{

   protected PreparedStatementProxy(ConnectionProxy connection, PreparedStatement statement)
   {
      super(connection, statement);
   }

}
