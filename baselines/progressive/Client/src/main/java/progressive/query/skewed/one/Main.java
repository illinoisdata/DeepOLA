package progressive.query.skewed.one;

import javax.annotation.processing.SupportedSourceVersion;
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
                        "select progressive l_returnflag, " +
                                "l_linestatus, " +
                                "sum(l_quantity) as sum_qty, " +
                                "sum(l_extendedprice) as sum_base_price, " +
                                "sum(l_extendedprice * (100 - l_discount)) as sum_disc_price, " +
                                "sum(l_extendedprice * (100 - l_discount) * (100 + l_tax)) as sum_charge, " +
                                "count(*) as count_order," +
                                "avg(l_quantity) as avg_qty," +
                                "avg(l_extendedprice) as avg_price," +
                                "avg(l_discount) as avg_disc," +
                                "progressive_partition()," +
                                "progressive_progress() " +
                                "from skewed " +
                                "where l_shipdate <= '1999-03-01' and l_shipdate >= '1998-09-02' " +
                                "group by l_returnflag, l_linestatus " +
                                "order by l_returnflag, l_linestatus "
                )) {

                    System.out.printf("%d,%s,%s,%f,%f,%f,%f,%f,%f,%f,%f,%f,%s\n",
                            0, "", "",
                            0.0, 0.0, 0.0, 0.0,
                            0.0, 0.0, 0.0,
                            0.0, 0.0, Instant.now().toString());

                    while (result.next()) {

                        final String return_flag = result.getString(1);
                        final String line_status = result.getString(2);
                        final double sum_qty = result.getDouble(3);
                        final double sum_base_price = result.getDouble(4);
                        final double sum_disc_price = result.getDouble(5);
                        final double sum_charge = result.getDouble(6);
                        final double count_order = result.getDouble(7);
                        final double avg_qty = result.getDouble(8);
                        final double avg_price = result.getDouble(9);
                        final double avg_disc = result.getDouble(10);
                        final int partition = result.getInt(11);
                        final double progress = result.getDouble(12);

                        System.out.printf("%d,%s,%s,%f,%f,%f,%f,%f,%f,%f,%f,%f,%s\n",
                                partition, return_flag, line_status,
                                sum_qty, sum_base_price, sum_disc_price, sum_charge,
                                avg_qty, avg_price, avg_disc,
                                count_order, progress, Instant.now().toString());

                    }
//                    System.out.println("");
                }

            }
        }
    }
}
