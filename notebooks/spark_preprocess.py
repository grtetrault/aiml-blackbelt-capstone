import pyspark
import pyspark.ml as ml
import pyspark.sql as sql
import pyspark.sql.types as types
import pyspark.sql.functions as F

import boto3
import argparse

def main():
    parser = argparse.ArgumentParser(description="Preprocessor inputs and outputs")
    parser.add_argument("--input_bucket", type=str, help="S3 bucket for input data")
    parser.add_argument("--input_dir", type=str, help="S3 prefix for input data")
    parser.add_argument("--output_bucket", type=str, help="S3 bucket for output data")
    parser.add_argument("--output_dir", type=str, help="S3 prefix for output data")
    args = parser.parse_args()
    
    spark = SparkSession.builder.appName("PySparkApp").getOrCreate()

    # This is needed to save RDDs which is the only way to write nested Dataframes into CSV format.
    spark.sparkContext._jsc.hadoopConfiguration().set(
        "mapred.output.committer.class", "org.apache.hadoop.mapred.FileOutputCommitter"
    )
    
    
    # _________________________________________________________________________
    #                                                           Read and filter
    
    # Subset ETL output to only relevant columns for model input.
    join_df = spark.read.parquet(f"s3://{args.input_bucket}/{args.input_dir}/")
    input_df = (
        join_df
        # Remove line items that contain no weather data.
        .filter(
            F.col("origin_station_id").isNotNull() 
            & F.col("dest_station_id").isNotNull()
        )
        .select(
            "arr_delay", "cancelled",
            "fl_date", "month", "day_of_month", "day_of_week",
            "origin", "origin_state_abr",
            "dest", "dest_state_abr",
            "op_carrier",
            "origin_tmax", "origin_tmin", "origin_prcp", "origin_snow", "origin_snwd",
            "dest_tmax", "dest_tmin", "dest_prcp", "dest_snow", "dest_snwd"
        )
        # Fill NaN values where neccesary.
        .na.fill(
            0.0,
            subset=[
                "origin_prcp", "origin_snow", "origin_snwd",
                "dest_prcp", "dest_snow", "dest_snwd"
            ]
        )
        
        
    # _________________________________________________________________________
    #                                                                    Target
    
    input_df = (
        input_df
        .withColumn(
            "delay_status", 
            F.when((F.col("arr_delay") > 0) | (F.col("cancelled") == 1), 1)
            .otherwise(0)
        )
    )
        

    # _________________________________________________________________________
    #                                                               Categorical
    
    get_distinct_in_input = (
        lambda col:
        input_df.select(col).distinct().rdd.flatMap(lambda _: _).collect()
    )
        
    # One hot encode carriers.
    distinct_carriers = get_distinct_in_input("op_carrier")
    for carrier in distinct_carriers:
        input_df = input_df.withColumn(
            f"op_carrier_{carrier}", 
            F.when(F.col("op_carrier")==carrier, 1).otherwise(0)
        )
        
    # Encode origins and destinations by frequency.
    input_df = (
        input_df
        .withColumn(
            "origin_freq", 
            F.count("origin")
            .over(sql.Window().partitionBy("origin")) / input_df.count()
        )
        .withColumn(
            "dest_freq", 
            F.count("dest")
            .over(sql.Window().partitionBy("dest")) / input_df.count()
        )
    )
        
    # One hot encode origin states.
    distinct_origin_states = get_distinct_in_input("origin_state_abr")
    for state in distinct_origin_states:
        input_df = input_df.withColumn(
            f"origin_state_abr_{state}", 
            F.when(F.col("origin_state_abr")==state, 1).otherwise(0)
        )
        
    # One hot encode destination states.
    distinct_dest_states = get_distinct_in_input("dest_state_abr")
    for state in distinct_dest_states:
        input_df = input_df.withColumn(
            f"dest_state_abr_{state}", 
            F.when(F.col("dest_state_abr")==state, 1).otherwise(0)
        )
        
    
    # _________________________________________________________________________
    #                                                                Continuous
    weather_cols = [
        "origin_tmax", "origin_tmin", "origin_prcp", "origin_snow", "origin_snwd",
        "dest_tmax", "dest_tmin", "dest_prcp", "dest_snow", "dest_snwd"
    ]
    weather_col_assembler = ml.feature.VectorAssembler(
        inputCols=weather_cols, 
        outputCol="weather_features",
        handleInvalid="keep"
    )
    input_df = weather_col_assembler.transform(input_df)

    std_scalar = ml.feature.StandardScaler(
        inputCol="weather_features", 
        outputCol="weather_features_scaled"
    )
    std_scalar_model = std_scalar.fit(input_df)
    input_df = std_scalar_model.transform(input_df)
        
        
    # _________________________________________________________________________
    #                                                        Finalize and Split
        
    # Remove the non-engineered columns from the data set before exporting.
    input_df = input_df.drop(
        "arr_delay", "cancelled",
        "fl_date",
        "origin", "origin_state_abr",
        "dest", "dest_state_abr",
        "op_carrier",
        "origin_tmax", "origin_tmin", "origin_prcp", "origin_snow", "origin_snwd",
        "dest_tmax", "dest_tmin", "dest_prcp", "dest_snow", "dest_snwd",
        "weather_features", "weather_features_scaled"
    )
        
    # Create train and test splits by using stratified sampling on 
    # the delay status.
    train_pct = 0.80
    train_df = input_df.stat.sampleBy(
        "delay_status", 
        fractions={0: train_pct, 1: train_pct}, 
        seed=0
    )
    # Get all rows not in training set.
    test_df = input_df.join(train_df.select("id"), on="id", how="leftanti")
        
    # _________________________________________________________________________
    #                                                               Write to S3
    
    train_df.write.csv(f"s3://{args.output_bucket}/{args.output_dir}/train/")
    test_df.write.csv(f"s3://{args.output_bucket}/{args.output_dir}/test/")
