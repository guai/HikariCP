package ru.programpark.hikari.ha;

import ru.programpark.hikari.HikariConfig;
import ru.programpark.hikari.HikariDataSource;
import org.postgresql.ds.PGSimpleDataSource;

import java.sql.SQLException;

public class Test2
{

//	static SimpleProxy proxy = new SimpleProxy("localhost", 5433, 65433).started();

   public static void main(String... args) throws SQLException
   {

      try {
         PGSimpleDataSource ds1 = new PGSimpleDataSource();
         ds1.setUrl("jdbc:postgresql://localhost:5433/activiti");
         ds1.setUser("postgres");
         ds1.setPassword("postgres");

         PGSimpleDataSource ds2 = new PGSimpleDataSource();
         ds2.setUrl("jdbc:postgresql://localhost:5432/activiti");
         ds2.setUser("postgres");
         ds2.setPassword("postgres");

         HikariConfig cfg = new HikariConfig();
         cfg.setDataSource(ds1);
         cfg.setDataSource2(ds2);
         cfg.setTwinJmxUrl("localhost:9032");
         cfg.setPoolName("activitiHikariCP");

         cfg.setMaximumPoolSize(3);
//			cfg.setAutoCommit(true);
         cfg.setAutoCommit(false);

         HikariDataSource hikari = new HikariDataSource(cfg);

      }

      catch (Exception e) {
         e.printStackTrace();
      }

   }

}
