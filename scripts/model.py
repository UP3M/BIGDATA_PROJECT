from pyspark.sql import SparkSession

spark = SparkSession.builder\
        .appName("BDT Project")\
        .config("spark.sql.catalogImplementation","hive")\
        .config("hive.metastore.uris", "thrift://sandbox-hdp.hortonworks.com:9083")\
        .config("spark.sql.avro.compression.codec", "snappy")\
        .enableHiveSupport()\
        .getOrCreate()

sc = spark.sparkContext
sc.setLogLevel('WARN')

print(spark.catalog.listDatabases())

print(spark.catalog.listTables("projectdb"))

customers = spark.read.format("avro").table('projectdb.customers')
customers.createOrReplaceTempView('customers')

pings = spark.read.format("avro").table('projectdb.pings')
pings.createOrReplaceTempView('pings')

test = spark.read.format("avro").table('projectdb.test')
test.createOrReplaceTempView('test')

customers.printSchema()
pings.printSchema()
test.printSchema()

spark.sql("SELECT * FROM customers LIMIT 10").show()

spark.sql("SELECT * FROM pings LIMIT 10").show()

spark.sql("SELECT * FROM test LIMIT 10").show()

from pyspark.sql.window import Window
from pyspark.sql.functions import from_unixtime, col, lag, sum as sql_sum
from pyspark.sql.functions import row_number , when , round
from pyspark.sql.functions import monotonically_increasing_id, asc
from pyspark.sql.functions import min, max
from datetime import date
from pyspark.sql.functions import col, date_add, array_contains, lit , collect_list ,expr
from pyspark.sql.functions import to_date , date_format
from pyspark.sql.functions import dayofweek, dayofmonth, month, year
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler, MinMaxScaler
from pyspark.ml.regression import LinearRegression, RandomForestRegressor, GBTRegressor
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql.functions import round as pyspark_round
import time
from pyspark.ml.util import MLReader

custumers_df = customers
pings_df = pings

from pyspark.sql.functions import from_unixtime
from_unixtime, sum, lag, round
from pyspark.sql.window import Window
print("The whole session will take some time to finish, please wait")
print("Data Preprocessing Process")
# sorting by id and timestamp
pings_df = pings_df.sort(col("id"), col("timestamp")). \
           withColumn("timestamp_decode", from_unixtime(col("timestamp")))

# create a new DataFrame and drop duplicates
temp_ping_df = pings_df.dropDuplicates(). \
               withColumn("date", col("timestamp_decode").cast("date")). \
               withColumn("prev_timestamp", lag(col("timestamp")).over(Window.partitionBy("id", "date").orderBy("timestamp")))
temp_ping_df = temp_ping_df.withColumn("online_hours", (temp_ping_df.timestamp - temp_ping_df.prev_timestamp) / (60 * 60)).\
               fillna(0, subset=["prev_timestamp", "online_hours"])
# group by id and date to get total online hours per day
train_df = temp_ping_df \
    .groupBy("id", "date") \
    .agg(sql_sum("online_hours").alias("online_hours")) \
    .withColumn("online_hours", round(col("online_hours"), 1))


# Convert the "date" column to string and filter out dates after '2017-06-21'
train_df = train_df.withColumn("date", col("date").cast("string")) \
                   .filter(col("date") < "2017-06-22")

# Create a DataFrame for all dates from 2017-06-01 to 2017-06-21
num_days = (date(2017, 6, 21) - date(2017, 6, 1)).days + 1
train_date_df = spark.range(num_days).selectExpr("date_add('2017-06-01', cast(id as int)) as date") \
                   .withColumn("date", col("date").cast("date"))

# Create a DataFrame with one row per id and all dates for that id
ids_group = train_df.groupBy("id").agg(collect_list("date").alias("dates"))

# Create a DataFrame with all combinations of ids and dates not in the original DataFrame
zero_df = train_date_df.crossJoin(ids_group.select("id")) \
                  .join(ids_group, ["id"], "left_outer") \
                  .withColumn("dates", expr("transform(dates, x -> to_date(x))")) \
                  .where(~array_contains(col("dates"), col("date"))) \
                  .withColumn("online_hours", lit(0)) \
                  .select("id", "date", "online_hours")

# Concatenate the original DataFrame with the zero DataFrame
train_df = train_df.select("id", "date", "online_hours").union(zero_df) \
                   .sort("id", "date")

# Merge the customer dataframe with train dataframe
temp_df = custumers_df.join(train_df, on='id', how='outer')

# Remove rows with null values
temp_df = temp_df.dropna()

# Convert 'gender' column to binary
temp_df = temp_df.withColumn('gender', when(temp_df.gender == 'MALE', 1).otherwise(0))


# Convert 'date' column to datetime
temp_df = temp_df.withColumn('date', to_date('date', 'yyyy-MM-dd'))

# Extract date features
temp_df = temp_df.withColumn('day_name', dayofweek(temp_df.date))
temp_df = temp_df.withColumn('day', dayofmonth(temp_df.date))
temp_df = temp_df.withColumn('month', month(temp_df.date))
temp_df = temp_df.withColumn('year', year(temp_df.date))

# Map day and month names to integers
week_names = {'Sunday': 0, 'Monday': 1, 'Tuesday': 2, 'Wednesday': 3, 'Thursday': 4, 'Friday': 5, 'Saturday': 6}
month_names = {'January': 0, 'February': 1, 'March': 2, 'April': 3, 'May': 4, 'June': 5, 'July': 6,
               'August': 7, 'September': 8, 'October': 9, 'November': 10, 'December': 11}



temp_df = temp_df.withColumn('day_name', when(temp_df['day_name'] == 'Sunday', week_names['Sunday'])
                            .otherwise(when(temp_df['day_name'] == 'Monday', week_names['Monday'])
                            .otherwise(when(temp_df['day_name'] == 'Tuesday', week_names['Tuesday'])
                            .otherwise(when(temp_df['day_name'] == 'Wednesday', week_names['Wednesday'])
                            .otherwise(when(temp_df['day_name'] == 'Thursday', week_names['Thursday'])
                            .otherwise(when(temp_df['day_name'] == 'Friday', week_names['Friday'])
                            .otherwise(when(temp_df['day_name'] == 'Saturday', week_names['Saturday'])
                            .otherwise(temp_df['day_name']))))))))

# Replace month names with their corresponding integer values
temp_df = temp_df.withColumn('month', when(temp_df['month'] == 'January', month_names['January'])
                            .otherwise(when(temp_df['month'] == 'February', month_names['February'])
                            .otherwise(when(temp_df['month'] == 'March', month_names['March'])
                            .otherwise(when(temp_df['month'] == 'April', month_names['April'])
                            .otherwise(when(temp_df['month'] == 'May', month_names['May'])
                            .otherwise(when(temp_df['month'] == 'June', month_names['June'])
                            .otherwise(when(temp_df['month'] == 'July', month_names['July'])
                            .otherwise(when(temp_df['month'] == 'September', month_names['September'])
                            .otherwise(when(temp_df['month'] == 'October', month_names['October'])
                            .otherwise(when(temp_df['month'] == 'November', month_names['November'])
                            .otherwise(when(temp_df['month'] == 'December', month_names['December'])
                            .otherwise(when(temp_df['month'] == 'August', month_names['August'])
                            .otherwise(temp_df['month'])))))))))))))

# Select relevant features and assemble them into a single feature vector
feature_cols = ['gender', 'age', 'number_of_kids', 'day_name', 'day', 'month', 'year']
assembler = VectorAssembler(inputCols=feature_cols, outputCol='features')
temp_df = assembler.transform(temp_df).select('features', 'online_hours')
# Split data into training and testing sets
(training_data, testing_data) = temp_df.randomSplit([0.8, 0.2], seed=2021)


# Scale the feature vector
scaler = MinMaxScaler(inputCol='features', outputCol='scaledFeatures')
scalerModel = scaler.fit(training_data)
training_data = scalerModel.transform(training_data)
testing_data = scalerModel.transform(testing_data)
training_data.show()
testing_data.show()
training_data = training_data.withColumn('label', training_data.online_hours)

training_data = training_data.drop('features')
training_data = training_data.drop('online_hours')

training_data = training_data.withColumn('features', training_data.scaledFeatures)

training_data = training_data.drop('scaledFeatures')

column_names = training_data.columns
print(column_names)

# Fitting and modeling
print('Working on base models and fitting begins.....')

testing_data = testing_data.withColumn('label', testing_data.online_hours)
testing_data = testing_data.drop('features')
testing_data = testing_data.drop('online_hours')
testing_data = testing_data.withColumn('features', testing_data.scaledFeatures)
testing_data = testing_data.drop('scaledFeatures')

from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator

# Instantiate a RandomForestRegressor model
rf = RandomForestRegressor(featuresCol="features", labelCol="label")

# Create a parameter grid to search over
param_grid = ParamGridBuilder() \
    .addGrid(rf.maxDepth, [5, 10, 20]) \
    .addGrid(rf.numTrees, [10, 20, 30]) \
    .build()

# Create a cross-validator with the RandomForestRegressor model and the parameter grid
crossval = CrossValidator(estimator=rf,
                          estimatorParamMaps=param_grid,
                          evaluator=RegressionEvaluator(labelCol="label", predictionCol="prediction", metricName="rmse"),
                          numFolds=3)

# Fit the cross-validator to the training data
print("Now training Random Forest")
t = time.time()
cv_model = crossval.fit(training_data)

results_rf = []
print("Result:")
for params, avg_metric in zip(param_grid, cv_model.avgMetrics):
    result = {
        'Parameters': str(params),
        'Average RMSE': str(avg_metric)
    }
    results_rf.append(result)
    print("Parameters:", params)
    print("Average RMSE:", avg_metric)
    print()

# Convert results to a DataFrame
results_rf_df = spark.createDataFrame(results_rf)

# Save results as a CSV file
results_rf_df.coalesce(1)\
    .write\
    .mode("overwrite")\
    .format("csv")\
    .option("sep", ",")\
    .option("header","true")\
    .csv("output/q6.csv")

# Get the best model from the cross-validator
best_model = cv_model.bestModel

# Use the best model to make predictions on the test data
print("Now testing Random Forest")
predictions = best_model.transform(testing_data)

# Evaluate the performance of the best model using RMSE
evaluator = RegressionEvaluator(labelCol="label", predictionCol="prediction", metricName="rmse")
rmse = evaluator.evaluate(predictions)
print("RMSE: %.3f" % rmse)
print('Time elapsed is: {} sec'.format(time.time() - t))
print('\n\n\n*****************************\n\n\n')

# Save the best model for each algorithm

best_model.save('models/best_model_rf.model')

from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator

# Instantiate a LinearRegression model
lr = LinearRegression(featuresCol="features", labelCol="label")

# Create a parameter grid to search over
param_grid_lr = ParamGridBuilder() \
    .addGrid(lr.maxIter, [5, 10, 20]) \
    .addGrid(lr.regParam, [1.0, 0.1, 0.01]) \
    .build()

# Create a cross-validator with the LinearRegression model and the parameter grid
crossval_lr = CrossValidator(estimator=lr,
                          estimatorParamMaps=param_grid_lr,
                          evaluator=RegressionEvaluator(labelCol="label", predictionCol="prediction", metricName="rmse"),
                          numFolds=3)

# Fit the cross-validator to the training data
print("Now traing Linear")
t = time.time()
cv_model_lr = crossval_lr.fit(training_data)

# Print results for each parameter grid
results = []
print("Result:")
for params, avg_metric in zip(param_grid_lr, cv_model_lr.avgMetrics):
    result = {
        'Parameters': str(params),
        'Average RMSE': str(avg_metric)
    }
    results.append(result)
    print("Parameters:", params)
    print("Average RMSE:", avg_metric)
    print()

# Convert results to a DataFrame
results_df = spark.createDataFrame(results)

# Save results as a CSV file
results_df.coalesce(1)\
    .write\
    .mode("overwrite")\
    .format("csv")\
    .option("sep", ",")\
    .option("header","true")\
    .csv("output/q7.csv")
# Get the best model from the cross-validator
best_model_lr = cv_model_lr.bestModel

# Use the best model to make predictions on the test data
print("Now testing Linear")
predictions_lr = best_model_lr.transform(testing_data)

# Evaluate the performance of the best model using RMSE
evaluator = RegressionEvaluator(labelCol="label", predictionCol="prediction", metricName="rmse")
rmse = evaluator.evaluate(predictions_lr)
print("RMSE: %.3f" % rmse)
print('Time elapsed is: {} sec'.format(time.time() - t))
print('\n\n\n*****************************\n\n\n')
best_model_lr.save('models/best_model_lr.model')

