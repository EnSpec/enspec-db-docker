# UW EnSpec Database
`enspec-db-docker` is a docker container example of to create database skeleton of the tables used in EnSpec lab
Instructions below show how to set up the docker and ways to interact with it.

## Start the Docker
```
cd docker/enspec-db-docker
docker-compose up
```

## Initialize database
Note: this will wipe the database!

Once you start the docker in the above step, the following steps will help you to interact with the database either directly in your command prompt using SQL commands or through [PGDM-UI](https://github.com/ucd-library/pgdm-ui). After designing the database ERD, the table templates were generated using [pgdm template builder](https://github.com/ucd-library/pgdm/blob/master/docs/template-builder.md). Hence, the tables will work with the PGDM-UI.

First, add the following to your ~/.pg_service.conf file
```
[enspec-db-docker]
host=localhost
port=5433
user=postgres
dbname=postgres
```

Then set pg service variable
```
export PGSERVICE=enspec-db-docker
```

Finally, run import script
```
cd schema
./init.sh
```
