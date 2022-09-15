data_dir=data/scale=10/partition=1
size="128M"
cd ../
mkdir -p $data_dir/size=$size/
cd $data_dir
tables=("nation" "region" "supplier" "customer" "partsupp" "part" "orders" "lineitem")
for table in ${tables[@]};
	do
		split -C $size --numeric-suffixes=1 $table.tbl size=$size/$table.tbl.
	done
