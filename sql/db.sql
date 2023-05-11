-- switch to the database
\c project;

-- Optional
START TRANSACTION;

-- Add tables
-- cutomers table
CREATE TABLE customers (
    id VARCHAR ( 50 ) NOT NULL PRIMARY KEY,
    gender VARCHAR ( 50 ) NOT NULL,
    age integer,
    number_of_kids integer
);

-- pings table
CREATE TABLE pings(
    id VARCHAR ( 50 ) NOT NULL REFERENCES customers(id),
    timestamp BIGINT
);

CREATE TABLE test(
    id VARCHAR ( 50 ) NOT NULL REFERENCES customers(id),
    dates VARCHAR ( 50 ) NOT NULL,
    online_hours integer
);

-- No constraints to add

\COPY customers FROM 'data/customer_df.csv' DELIMITER ',' CSV HEADER NULL AS 'null';

\COPY pings FROM 'data/pings_df.csv' DELIMITER ',' CSV HEADER NULL AS 'null';

\COPY test FROM 'data/test_df.csv' DELIMITER ',' CSV HEADER NULL AS 'null';

-- optional
COMMIT;

-- For checking the content of tables
SELECT * from customers LIMIT 10;
SELECT * from pings LIMIT 10;
SELECT * from test LIMIT 10;
