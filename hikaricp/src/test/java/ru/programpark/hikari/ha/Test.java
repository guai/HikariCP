package ru.programpark.hikari.ha;

import ru.programpark.hikari.HikariConfig;
import ru.programpark.hikari.HikariDataSource;
import lombok.Cleanup;
import lombok.SneakyThrows;
import org.postgresql.ds.PGSimpleDataSource;

import javax.swing.*;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.InputStream;
import java.sql.*;

public class Test
{

   static SimpleProxy proxy = new SimpleProxy("localhost", 5433, 65433).started();

   public static void main(String... args) throws SQLException
   {

      PGSimpleDataSource ds1 = new PGSimpleDataSource();
      ds1.setUrl("jdbc:postgresql://localhost:5432/activiti");
      ds1.setUser("postgres");
      ds1.setPassword("postgres");

      PGSimpleDataSource ds2 = new PGSimpleDataSource();
      ds2.setUrl("jdbc:postgresql://localhost:65433/activiti");
      ds2.setUser("postgres");
      ds2.setPassword("postgres");

      HikariConfig cfg = new HikariConfig();
      cfg.setDataSource(ds1);
      cfg.setDataSource2(ds2);
      cfg.setPoolName("activitiHikariCP");
      cfg.setRegisterMbeans(true);
      cfg.setAllowPoolSuspension(true);
      cfg.setTwinJmxUrl("localhost:9033");

      cfg.setMaximumPoolSize(3);
//			cfg.setAutoCommit(true);
      cfg.setAutoCommit(false);
      final HikariDataSource hikari = new HikariDataSource(cfg);


      JFrame frame = new JFrame();
      frame.setDefaultCloseOperation(WindowConstants.EXIT_ON_CLOSE);
      frame.setBounds(200, 300, 150, 100);
      Button button = new Button("hit me");
      button.addActionListener(new ActionListener()
      {
         @Override
         @SneakyThrows
         public void actionPerformed(ActionEvent e)
         {
            proxy = new SimpleProxy("localhost", 5433, 65433).started();

            @Cleanup Connection con = hikari.getConnection();

            @Cleanup PreparedStatement pst = con.prepareStatement("INSERT INTO table1 VALUES(?)");
            @Cleanup InputStream blob = Test.class.getClassLoader().getResource("propfile3.properties").openStream();
            pst.setBinaryStream(1, blob);
            pst.execute();

            con.commit();
         }
      });
      frame.add(button);
      frame.setVisible(true);


      try {
         @Cleanup Connection con = hikari.getConnection();

         @Cleanup PreparedStatement pst = con.prepareStatement("INSERT INTO table1 VALUES(?)");
         @Cleanup InputStream blob = Test.class.getClassLoader().getResource("propfile1.properties").openStream();
         pst.setBinaryStream(1, blob);
         pst.execute();

         con.commit();
//			con.rollback();
         proxy.interrupt();

         @Cleanup PreparedStatement pst1 = con.prepareStatement("INSERT INTO table1 VALUES(?)");
         @Cleanup InputStream blob1 = Test.class.getClassLoader().getResource("propfile2.properties").openStream();
         pst1.setBinaryStream(1, blob1);
         pst1.execute();

         @Cleanup Statement st = con.createStatement();
         @Cleanup ResultSet rs = st.executeQuery("SELECT 1 AS id, 'foobar' AS name, current_timestamp AS now;");
         while (rs.next()) {
            System.out.print(rs.getLong(1));
            System.out.print(" ");
            System.out.print(rs.getString(2));
            System.out.print(" ");
            System.out.println(rs.getTimestamp(3));
         }

         con.commit();

      }

      catch (Exception e) {
         e.printStackTrace();
      }

   }

}
