if [ "$#" -ne 1 ]; then
	echo "Usage: ./1a-restart-postgres.sh <scale>"
	exit
fi
#variables
scale=$1 #scale of data
partition=$2 #number of partitions of data
variation=$3
container="pgsql-$scale-$variation" #container name
password="docker" #container password
port="5563" #port for PSQL
path="$(git rev-parse --show-toplevel)" #path to main directory

# Stop and remove all containers with pgsql- in the name
sudo docker stop $(sudo docker container ls -q --filter name=pgsql-*)
sudo docker rm pgsql-*

sudo docker run --rm --name $container -e POSTGRES_PASSWORD=$password -d -v "$path:$path" -v "/mnt/$container:/var/lib/postgresql/data" -v "/mnt/DeepOLA:/mnt/DeepOLA" -p 5432:5432 postgres
