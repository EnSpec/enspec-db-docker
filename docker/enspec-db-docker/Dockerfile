FROM postgres:10
MAINTAINER Justin Merz <jrmerz@ucdavis.edu>

RUN apt-get update \
      && apt-get install -y --no-install-recommends \
        postgis \
        postgresql-10-postgis-2.5 \
        postgresql-10-postgis-2.5-scripts \
        vim \
      && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /docker-entrypoint-initdb.d
COPY ./initdb-postgis.sql /docker-entrypoint-initdb.d/postgis.sql