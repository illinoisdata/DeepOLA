create index on customer using btree (c_mktsegment);
create index on orders using btree (o_custkey);
create index on lineitem using btree (l_orderkey);
create index on lineitem using btree (l_returnflag);
create index on nation using btree (n_name);
create index on supplier using btree (s_nationkey);
create index on customer using btree (c_nationkey);
create index on lineitem using btree (l_suppkey);
create index on lineitem using btree (l_shipdate);

-- Additional Indexes
create index on orders using btree (o_orderdate);
create index on region using btree (r_name);
create index on partsupp using btree (ps_suppkey);
create index on nation using btree (n_regionkey);