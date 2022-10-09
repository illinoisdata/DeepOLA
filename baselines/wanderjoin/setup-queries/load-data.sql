\copy "region" from "/mnt/DeepOLA/resources/tpc-h/data/scale\=10/partition\=1/tbl/region.tbl" DELIMITER '|' CSV
\copy "nation" from "/mnt/DeepOLA/resources/tpc-h/data/scale\=10/partition\=1/tbl/nation.tbl" DELIMITER '|' CSV
\copy "customer" from "/mnt/DeepOLA/resources/tpc-h/data/scale=10/partition=1/tbl/customer.tbl" DELIMITER '|' CSV
\copy "supplier" from "/mnt/DeepOLA/resources/tpc-h/data/scale=10/partition=1/tbl/supplier.tbl" DELIMITER '|' CSV
\copy "part" from "/mnt/DeepOLA/resources/tpc-h/data/scale=10/partition=1/tbl/part.tbl" DELIMITER '|' CSV
\copy "partsupp" from "/mnt/DeepOLA/resources/tpc-h/data/scale=10/partition=1/tbl/partsupp.tbl" DELIMITER '|' CSV
\copy "orders" from "/mnt/DeepOLA/resources/tpc-h/data/scale=10/partition=1/tbl/orders.tbl" DELIMITER '|' CSV
\copy "lineitem" from "/mnt/DeepOLA/resources/tpc-h/data/scale=10/partition=1/tbl/lineitem.tbl" DELIMITER '|' CSV
