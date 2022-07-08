package de.tuda.progressive.db.standalone;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.sql.ResultSet;

public class Client {

   public static void main(String[] args) throws Exception {
//           try (Connection connection = DriverManager.getConnection("jdbc:avatica:remote:url=http://localhost:9000")) {
//                   try (Statement statement = connection.createStatement()) {
//                           statement.execute("CREATE TABLE test (a INT, b VARCHAR(100))");
//                           statement.execute("INSERT INTO test VALUES (1, 'a')");
//                           statement.execute("INSERT INTO test VALUES (2, 'a')");
//                           statement.execute("INSERT INTO test VALUES (3, 'b')");
//                           statement.execute("INSERT INTO test VALUES (4, 'b')");
//                           statement.execute("PREPARE TABLE test");

//                           try (ResultSet result = statement.executeQuery("SELECT PROGRESSIVE AVG(a), b, PROGRESSIVE_PROGRESS() from test GROUP BY b")) {
//                                   while (result.next()) {
//                                           final double avg = result.getDouble(1);
//                                           final String group = result.getString(2);
//                                           final double progress = result.getDouble(3);
//
//                                           System.out.printf("%f | %s | %f\n", avg, group, progress);
//                                   }
//                           }
//                   }
//           }



try (Connection connection = DriverManager.getConnection("jdbc:avatica:remote:url=http://localhost:9000")) {
  try (Statement statement = connection.createStatement()) {
    statement.execute("CREATE TABLE test2 (a INT, b VARCHAR(100))");
    statement.execute("INSERT INTO test2 VALUES (1, 'a')");
    statement.execute("INSERT INTO test2 VALUES (2, 'a')");
    statement.execute("INSERT INTO test2 VALUES (3, 'b')");
    statement.execute("INSERT INTO test2 VALUES (4, 'b')");
    statement.execute("PREPARE TABLE test2");

    statement.execute("CREATE PROGRESSIVE VIEW pv AS SELECT AVG(a), b FUTURE, PROGRESSIVE_PROGRESS() FROM test2 WHERE (b = 'a') FUTURE OR (b = 'b') FUTURE GROUP BY b FUTURE");

    try (ResultSet result = statement.executeQuery("SELECT PROGRESSIVE * from pv WITH FUTURE WHERE b = 'b'")) {
      while (result.next()) {
        final double avg = result.getDouble(1);
        final double progress = result.getDouble(2);

        System.out.printf("%f | %f\n", avg, progress);
      }
    }

    try (ResultSet result = statement.executeQuery("SELECT PROGRESSIVE * from pv WITH FUTURE GROUP BY b")) {
      while (result.next()) {
        final double avg = result.getDouble(1);
        final String group = result.getString(2);
        final double progress = result.getDouble(3);

        System.out.printf("%f | %s | %f\n", avg, group, progress);
      }
    }
  }
}









   }
} 
