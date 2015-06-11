package com.zaxxer.hikari.proxy;

import java.sql.Statement;

abstract public class StatementProxy extends GenericStatementProxy<Statement>
{

   protected StatementProxy(ConnectionProxy connection, Statement statement)
   {
      super(connection, statement);
   }

}
