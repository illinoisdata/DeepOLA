#!/bin/bash

#container name = postgres-db
#password = docker
#port = 5432
#postgres version = 14.2
docker run --name postgres-db -e POSTGRES_PASSWORD=docker -p 5432:5432 -d postgres

#output container-id of Docker container postgres-db
docker ps -aqf "name=postgres-db"

#copy dataset file, 'file-name' from path to container with container-id
docker cp <path> <container-id>:/<file-name>

#connect to the PostgreSQL server in Docker container as user postgres
docker exec -it postgres-db psql -U postgres

#create database
CREATE DATABASE myDatabase;

#connect to database
\connect myDatabase;

#create table named Customer
CREATE TABLE Customer(
    num int, 
    custkey varchar(100), 
    name varchar(50), 
    address varchar(50), 
    phone varchar(50), 
    acctbal double precision, 
    mktsegment varchar(50), 
    comment varchar(255)
    );

#copy data from dataset file, 'file-name' with '|' delimiter into Customer table
\copy Customer from '/<file-name>' ( format csv, delimiter('|'));

#start outputting into <output-file>.txt
\o <output-file>.txt

#run query
SELECT custkey from Customer;

#output execution time of above query
EXPLAIN ANALYSE SELECT custkey from Customer;

#stop output to output.txt
\o

#exit PSQL command-line interface
exit

#copy <output-file>.txt from docker container to path
docker cp <container-id>:/<output-file>.txt <path>
