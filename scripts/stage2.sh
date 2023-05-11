#!/bin/bash
hdfs dfs -rm -r /project/avsc
hdfs dfs -mkdir /project/avsc
hdfs dfs -put /project/avsc/*.avsc /project/avsc
hive -f sql/db.hql
#hive -f db.hql > hive_results.txt
echo "gender, number_of_customer" > output/q1.csv
cat /root/q1/* >> output/q1.csv

echo "age,number_of_customer" > output/q2.csv
cat /root/q2/* >> output/q2.csv

echo "number_of_customer_kids, number_of_customer" > output/q3.csv
cat /root/q3/* >> output/q3.csv

echo "online_hours_of_customer, number_of_customer" > output/q4.csv
cat /root/q4/* >> output/q4.csv

echo "date, total_online_hours_of_customer" > output/q5.csv
cat /root/q5/* >> output/q5.csv
