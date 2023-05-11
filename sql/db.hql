DROP DATABASE IF EXISTS projectdb CASCADE;
CREATE DATABASE projectdb;
USE projectdb;

SET mapreduce.map.output.compress = true;
SET mapreduce.map.output.compress.codec = org.apache.hadoop.io.compress.SnappyCodec;

-- Create tables

-- customers table
CREATE EXTERNAL TABLE customers STORED AS AVRO LOCATION '/project/customers' TBLPROPERTIES ('avro.schema.url'='/project/avsc/customers.avsc');


-- pings table
CREATE EXTERNAL TABLE pings STORED AS AVRO LOCATION '/project/pings' TBLPROPERTIES ('avro.schema.url'='/project/avsc/pings.avsc');

-- test table
CREATE EXTERNAL TABLE test STORED AS AVRO LOCATION '/project/test' TBLPROPERTIES ('avro.schema.url'='/project/avsc/test.avsc');

-- For checking the content of tables
SELECT * from customers LIMIT 10;
SELECT * from pings LIMIT 10;
SELECT * from test LIMIT 10;

SET hive.enforce.bucketing=true;

CREATE EXTERNAL TABLE customer_buck(
    id int, 
    gender varchar(50),
    age int, 
    number_of_kids int
) 
    CLUSTERED BY (id) into 5 buckets
    STORED AS AVRO LOCATION '/project/customer_buck' 
    TBLPROPERTIES ('AVRO.COMPRESS'='SNAPPY');

-- EDA
-- 1. Show the distribution of customers gender.
INSERT OVERWRITE LOCAL DIRECTORY '/root/q1'
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ','
SELECT gender, COUNT(*) AS count
FROM customers
GROUP BY gender;

-- 2. Show the distribution of customers age.
INSERT OVERWRITE LOCAL DIRECTORY '/root/q2'
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ','
SELECT age, COUNT(*) AS count
FROM customers
GROUP BY age;

-- 3. Show the distribution of customers kids.
INSERT OVERWRITE LOCAL DIRECTORY '/root/q3'
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ','
SELECT number_of_kids, COUNT(*) AS count
FROM customers
GROUP BY number_of_kids;

-- 4. Show the distribution of customers online hours.
INSERT OVERWRITE LOCAL DIRECTORY '/root/q4'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT online_hours, COUNT(*) AS count
FROM test
GROUP BY online_hours;

-- 5. Show at which date customer online the most
INSERT OVERWRITE LOCAL DIRECTORY '/root/q5'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT dates, SUM(online_hours) AS total_online_hours
FROM test
GROUP BY dates;
