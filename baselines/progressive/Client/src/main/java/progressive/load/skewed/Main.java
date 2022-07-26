package progressive.load.skewed;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

public class Main {
    public static void main(String[] args) throws Exception {
        try (Connection connection = DriverManager.getConnection("jdbc:avatica:remote:url=http://localhost:9000")) {
            try (Statement statement = connection.createStatement()) {

//                statement.execute("CREATE TABLE IF NOT EXISTS skewed ( l_orderkey int, l_partkey int, l_suppkey int, l_linenumber int, l_quantity int, l_extendedprice int, l_discount int, l_tax int, l_returnflag VARCHAR(1), l_linestatus VARCHAR(1), l_shipdate VARCHAR(44), l_commitdate VARCHAR(44), l_receiptdate VARCHAR(44), l_shipinstruct VARCHAR(44), l_shipmode VARCHAR(44), l_comment VARCHAR(44))");

//                statement.execute("INSERT INTO lineitem VALUES (1, 155190, 7706, 1, 1700, 21168, 4, 2, 'N', 'O', '1996-03-13', '1996-02-12', '1996-03-22', 'DELIVER IN PERSON', 'TRUCK', 'egular courts above the')");
//                statement.execute("INSERT INTO lineitem VALUES (1, 67310, 7311, 2, 3600, 45983, 9, 6, 'N', 'O', '1996-04-12', '1996-02-28', '1996-04-20', 'TAKE BACK RETURN', 'MAIL', 'ly final dependencies: slyly bold')");
//                statement.execute("INSERT INTO lineitem VALUES (1, 63700, 3701, 3, 800, 13310, 10, 2, 'N', 'O', '1996-01-29', '1996-03-05', '1996-01-31', 'TAKE BACK RETURN', 'REG AIR', 'riously. regular, express dep')");
//                statement.execute("INSERT INTO lineitem VALUES (1, 2132, 4633, 4, 2800, 28956, 9, 6, 'N', 'O', '1996-04-21', '1996-03-30', '1996-05-16', 'NONE', 'AIR', 'lites. fluffily even de')");

                statement.execute("PREPARE TABLE skewed");

            }
        }
    }
}
