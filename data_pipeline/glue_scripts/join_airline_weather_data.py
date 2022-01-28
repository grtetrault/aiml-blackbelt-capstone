import sys
from awsglue.job import Job
from awsglue.transforms import Join
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from awsglue.dynamicframe import DynamicFrame

import pyspark.sql as sql
import pyspark.sql.functions as F
from pyspark.context import SparkContext

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)

# Read in job parameters and unpack.
args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "dl_output_glue_db", "etl_output_bucket"]
)

# Unpack parameters set in stack.
glue_db = args["dl_output_glue_db"]
output_bucket = args["etl_output_bucket"]

# Table names are determined by directory set in the data fetching lambda 
# functions' environement variables in stack creation.
airline_tbl = "airline_data"
weather_tbl = "weather_data"
station_tbl = "station_data"
airport_tbl = "airport_data"


     
# Destination for fully joined dataframe.
etl_output_loc = f"s3://{output_bucket}/etl_output/joined_airline_weather_data"

# Create dynamic frames from source tables and convert to Spark dataframes.
airline_dyf = glueContext.create_dynamic_frame.from_catalog(database=glue_db, table_name=airline_tbl)
weather_dyf = glueContext.create_dynamic_frame.from_catalog(database=glue_db, table_name=weather_tbl)
station_dyf = glueContext.create_dynamic_frame.from_catalog(database=glue_db, table_name=station_tbl)
airport_dyf = glueContext.create_dynamic_frame.from_catalog(database=glue_db, table_name=airport_tbl)

airline_df = airline_dyf.toDF()
weather_df = weather_dyf.toDF()
station_df = station_dyf.toDF()
airport_df = airport_dyf.toDF()

def geo_distance(lat1, long1, lat2, long2):
    """
    Calculate the great circle distance between two locations.
    For more details, see: https://en.wikipedia.org/wiki/Haversine_formula
    """
    lat1, long1, lat2, long2 = map(F.radians, [lat1, long1, lat2, long2])

    # Haversine formula.
    hav_lat = F.sin((lat2 - lat1) / 2.0) ** 2
    hav_long =  F.sin((long2 - long1) / 2.0) ** 2
    
    radius = 6371.0 # Radius of Earth in kilometers.
    dist = 2.0 * radius * F.asin(F.sqrt(hav_lat + F.cos(lat1) * F.cos(lat2) * hav_long))
    return dist

# The range a station can be away from an airport and maximum number of
# stations to consider.
station_search_radius = 250 # kilometers.
n_nearest_stations = 5

# Join station data to airport data by geographically closest station.
iata_window = (
    sql.window.Window
    .partitionBy(*airport_df.columns)
    .orderBy(F.col("geo_distance").asc())
)
airport_station_df = (
    airport_df
    .join(
        station_df.select(
            F.col("id").alias("station_id"), 
            F.col("latitude").alias("station_latitude"), 
            F.col("longitude").alias("station_longitude")
        ),
        on=geo_distance(
            F.col("latitude"), F.col("longitude"), 
            F.col("station_latitude"), F.col("station_longitude")
        ) <= station_search_radius,
        how="left"
    )
    .withColumn("geo_distance", 
        geo_distance(
            F.col("latitude"), F.col("longitude"), 
            F.col("station_latitude"), F.col("station_longitude")
        )
    )
    .withColumn("row_number", F.row_number().over(iata_window))
    .filter(F.col("row_number") <= n_nearest_stations)
    .drop("row_number")
    .groupby("iata", "latitude", "longitude")
    .agg(F.collect_list("station_id").alias("in_range_station_ids"))
)

# Join station ID and additional airport information to airline data.
geo_airline_df = (
    airline_df
    .join(
        airport_station_df.select(
            F.col("iata").alias("origin"),
            F.col("latitude").alias("origin_latitude"),
            F.col("longitude").alias("origin_longitude"),
            F.col("in_range_station_ids").alias("origin_in_range_station_ids")
        ),
        on="origin",
        how="left"
    )
    .join(
        airport_station_df.select(
            F.col("iata").alias("dest"),
            F.col("latitude").alias("dest_latitude"),
            F.col("longitude").alias("dest_longitude"),
            F.col("in_range_station_ids").alias("dest_in_range_station_ids")
        ),
        on="dest",
        how="left"
    )
    .withColumn("date_timestamp", F.to_date(F.col("fl_date"), "yyyy-MM-dd"))
)

# Create UDF to subset weather data to only relevant stations.
in_range_station_id_values = sc.broadcast(
    set(
        airport_station_df
        .select(F.explode("in_range_station_ids"))
        .rdd
        .flatMap(lambda x: x)
        .distinct()
        .collect()
    )
)
is_in_range_station_udf = F.udf(
    lambda x: x in in_range_station_id_values.value, 
    sql.types.BooleanType()
)

# Join weather data corresponding to date and station to airline data.
weather_pivot_df = (
    weather_df
    .filter(is_in_range_station_udf(F.col("id")))
    .filter(F.col("element").isin(["PRCP", "SNOW", "SNWD", "TMAX", "TMIN"]))
    .groupby("id", "year_month_day")
    .pivot("element")
    .avg("data_value")
    .join(
        station_df.select(
            F.col("id"),
            F.col("latitude"),
            F.col("longitude")
        ),
        on="id",
        how="left"
    )
    .filter(F.col("TMIN").isNotNull() & F.col("TMAX").isNotNull())
    .withColumn("date_timestamp", F.to_date(F.col("year_month_day"), "yyyyMMdd"))
)

# Join weather data to airlines by closest in range station for each date. Note
# that this must be done for each flight's orgin and destination.
airline_window = (
    sql.window.Window
    .partitionBy(*geo_airline_df.columns)
    .orderBy(F.col("geo_distance").asc())
)

airline_weather_df = (
    geo_airline_df
    .join(
        weather_pivot_df.select(
            F.col("date_timestamp").alias("origin_station_date_timestamp"),
            F.col("id").alias("origin_station_id"),
            F.col("latitude").alias("origin_station_latitude"),
            F.col("longitude").alias("origin_station_longitude"),
            F.col("PRCP").alias("origin_prcp"),
            F.col("SNOW").alias("origin_snow"),
            F.col("SNWD").alias("origin_snwd"),
            F.col("TMAX").alias("origin_tmax"),
            F.col("TMIN").alias("origin_tmin")
        ),
        on=(
            (F.col("date_timestamp")==F.col("origin_station_date_timestamp"))
            & F.expr("array_contains(origin_in_range_station_ids, origin_station_id)")
        ),
        how="left"
    )
    .withColumn("geo_distance", 
        geo_distance(
            F.col("origin_latitude"), F.col("origin_longitude"), 
            F.col("origin_station_latitude"), F.col("origin_station_longitude")
        )
    )
    .withColumn("row_number", F.row_number().over(airline_window))
    .filter(F.col("row_number") == 1)
    .drop("row_number")
    .withColumnRenamed("geo_distance", "origin_geo_distance")

    # Apply same join operation as above to destination.
    .join(
        weather_pivot_df.select(
            F.col("date_timestamp").alias("dest_station_date_timestamp"),
            F.col("id").alias("dest_station_id"),
            F.col("latitude").alias("dest_station_latitude"),
            F.col("longitude").alias("dest_station_longitude"),
            F.col("PRCP").alias("dest_prcp"),
            F.col("SNOW").alias("dest_snow"),
            F.col("SNWD").alias("dest_snwd"),
            F.col("TMAX").alias("dest_tmax"),
            F.col("TMIN").alias("dest_tmin")
        ),
        on=(
            (F.col("date_timestamp")==F.col("dest_station_date_timestamp"))
            & F.expr("array_contains(dest_in_range_station_ids, dest_station_id)")
        ),
        how="left"
    )
    .withColumn("geo_distance", 
        geo_distance(
            F.col("dest_latitude"), F.col("dest_longitude"), 
            F.col("dest_station_latitude"), F.col("dest_station_longitude")
        )
    )
    .withColumn("row_number", F.row_number().over(airline_window))
    .filter(F.col("row_number") == 1)
    .drop("row_number")
    .withColumnRenamed("geo_distance", "dest_geo_distance")
)

# Turn result back into a dynamic frame.
airline_weather_dyf = DynamicFrame.fromDF(airline_weather_df, glueContext, "airline_weather_data")

# Purge destination to remove stale data.
glueContext.purge_s3_path(etl_output_loc)

# Write out newly created dynamic frame in Parquet.
glueContext.write_dynamic_frame.from_options(
    airline_weather_dyf, 
    connection_type="s3",
    connection_options = {"path": etl_output_loc},
    format = "parquet"
)