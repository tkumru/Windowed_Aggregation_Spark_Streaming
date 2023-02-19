from pyspark.sql import SparkSession, functions as F

import findspark

findspark.init("C:\Program Files\Spark\spark-3.3.1-bin-hadoop3")

#####################################
# !python dataframe_to_log.py -idx True -i datasets/iot_telemetry_data.csv -o datasets/output
#####################################

spark = (SparkSession.builder
         .master("yarn")
         .appName("Windowed Aggregation")
         .config("spark.sql.adaptive", True)
         .config("spark.sql.shuffle.partitions", 5)
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

#####################################
# Create Schema
#####################################

iot_schema = "row_id int, " \
    "ts double, " \
    "device string, " \
    "co float, " \
    "humidity float, " \
    "light boolean, " \
    "lpg float, " \
    "motion boolean, " \
    "smoke float, " \
    "temp float, " \
    "time timestamp"
    
#####################################
# Read Stream to Output File
#####################################

path = "file:///Users/talha/OneDrive/Masaüstü/Talha Nebi Kumru/Data Enginnering/Miuul/RealTimeDPWithSpark/Windowed_Aggregation_Spark_Streaming/datasets/output"

file = (spark.readStream
        .format("csv")
        .schema(iot_schema)
        .option("header", True)
        .option("maxFilesPerTrigger", 1)
        .load(path))

#####################################
# Operations
#####################################

windowed_file = file.groupBy(F.window(F.col("time"), "10 minutes", "5 minutes"),
                             F.col("device")).agg(
                                     F.count("device").alias("Count"),
                                     F.avg("humidity").alias("ten_min_avg(humidity)"),
                                     F.avg("co").alias("ten_min_avg(humidity)"))
                                 
#####################################
# Write File Stream to Sink
#####################################

check_path = "file:///Users/talha/OneDrive/Masaüstü/Talha Nebi Kumru/Data Enginnering/Miuul/RealTimeDPWithSpark/Windowed_Aggregation_Spark_Streaming/checkpoint"

streaming_query = (windowed_file.writeStream
                   .format("console")
                   .outputMode("complete")
                   .trigger(processingTime="2 seconds")
                   .option("numRows", 4)
                   .option("truncate", False)
                   .option("checkpointLocation", check_path)
                   .start())
streaming_query.awaitTermination()

#spark.stop()
