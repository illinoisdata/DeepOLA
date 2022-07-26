package progressive.query.skewed.six;

import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.ResultSet;
import java.time.Instant;


public class Main {
    public static void main(String[] args) throws Exception {
        try (Connection connection = DriverManager.getConnection("jdbc:avatica:remote:url=http://localhost:9000")) {
            try (Statement statement = connection.createStatement()) {

                try (ResultSet result = statement.executeQuery(
                        "select progressive sum(l_extendedprice * l_discount) as revenue, " +
                                "progressive_partition()," +
                                "progressive_progress()" +
                                "from skewed " +
                                "where l_shipdate >= '1995-01-01' " +
                                "and l_shipdate < '1996-01-01' " +
                                "and l_discount >= 5 AND l_discount <= 7" +
                                "and l_quantity < 40"
                )) {

                    System.out.printf("%d,%f,%f,%s \n", 0, 0.0, 0.0, Instant.now().toString());

                    while (result.next()) {

                        final double revenue = result.getDouble(1);
                        final int partition = result.getInt(2);
                        final double progress = result.getDouble(3);

                        System.out.printf("%d,%f,%f,%s \n", partition, revenue, progress, Instant.now().toString());

                    }
                }

            }
        }
    }
}
