if [ "$#" -ne 1 ]; then
	echo "Usage: ./1a-restart-postgres.sh <scale>"
	exit
fi
#variables
scale=$1 #scale of data
container="pgsql-$scale" #container name
password="docker" #container password
port="5563" #port for PSQL
path="$(git rev-parse --show-toplevel)" #path to main directory

sudo docker run --name $container -e POSTGRES_PASSWORD=$password -d -v "$path:$path" -v "/mnt/postgres-$scale:/var/lib/postgresql/data" -v "/mnt/DeepOLA:/mnt/DeepOLA" postgres
