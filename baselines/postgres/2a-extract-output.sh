if [ "$#" -ne 1 ]; then
	echo "Usage: ./2a-extract-output.sh <scale>"
	exit
fi
scale=$1
output_dir=/mnt/DeepOLA/resources/tpc-h/data/scale\=$scale/original/
cp /mnt/postgres-$scale/*.csv $output_dir/
chown -R deepola $output_dir/
chgrp -R deepola $output_dir/
