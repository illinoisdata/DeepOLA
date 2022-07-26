package progressive.query.lineitem.one;

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
                        "select progressive sum(l_quantity) as sum_qty, " +
                                "sum(l_extendedprice) as sum_base_price, " +
                                "sum(l_extendedprice * (100 - l_discount)) as sum_disc_price, " +
                                "sum(l_extendedprice * (100 - l_discount) * (100 + l_tax)) as sum_charge, " +
                                "avg(l_quantity) as avg_qty, " +
                                "avg(l_extendedprice) as avg_price, avg(l_discount) as avg_disc, " +
                                "l_returnflag, l_linestatus," +
                                "progressive_partition(), " +
                                "PROGRESSIVE_PROGRESS() " +
                                "from lineitem " +
                                "group by l_returnflag, l_linestatus " +
                                "order by l_returnflag, l_linestatus"
                )) {

                    System.out.printf("%d,%f,%f,%f,%f,%f,%f,%f,%s,%s,%f,%s\n",
                            0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, "", "", 0.0, Instant.now().toString());

                    while (result.next()) {

                        final double sum_qty = result.getDouble(1);
                        final double sum_base_price = result.getDouble(2);
                        final double sum_disc_price = result.getDouble(3);
                        final double sum_charge = result.getDouble(4);
                        final double avg_qty = result.getDouble(5);
                        final double avg_price = result.getDouble(6);
                        final double avg_disc = result.getDouble(7);
                        final String return_flag = result.getString(8);
                        final String line_status = result.getString(9);
                        final int partition = result.getInt(10);
                        final double progress = result.getDouble(11);

                        System.out.printf("%d,%f,%f,%f,%f,%f,%f,%f,%s,%s,%f,%s\n",
                                partition, sum_qty, sum_base_price, sum_disc_price, sum_charge, avg_qty, avg_price, avg_disc, return_flag, line_status, progress, Instant.now().toString());

                    }
                    System.out.println("");
                }

            }
        }
    }
}