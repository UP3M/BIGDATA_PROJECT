#!/bin/bash
psql -U postgres -c 'DROP DATABASE IF EXISTS project;'
psql -U postgres -c 'CREATE DATABASE project;'
psql -U postgres -d project -f sql/db.sql
rm -r /project
hdfs dfs -rm -r /project

sqoop list-tables \
    --connect jdbc:postgresql://localhost/project \
    --username postgres

sqoop eval \
    --connect jdbc:postgresql://localhost/project \
    --username postgres \
    --query "SELECT * FROM customers LIMIT 10"

sqoop eval \
    --connect jdbc:postgresql://localhost/project \
    --username postgres \
    --query "SELECT * FROM test LIMIT 10"

sqoop import-all-tables \
    -Dmapreduce.job.user.classpath.first=true \
    --connect jdbc:postgresql://localhost/project \
    --username postgres \
    --warehouse-dir /project \
    --as-avrodatafile \
    --compression-codec=snappy \
    --outdir /project/avsc \
    --m 1
