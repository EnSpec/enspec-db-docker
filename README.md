# enspec-db
UW EnSpec Database

# Run Dev

```
cd docker/enspec-db-dev
docker-compose up
```

# Initialize database

Note: this will wipe the database!

First, add the following to your ~/.pg_service.conf file
```
[enspecdev]
host=localhost
port=5433
user=postgres
dbname=postgres
```

Then set pg service variable
```
export PGSERVICE=enspecdev
```

Finally, run import script
```
cd schema
./init.sh
```