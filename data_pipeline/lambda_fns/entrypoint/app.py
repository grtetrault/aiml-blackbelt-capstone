import os
import io
import re
import boto3
import typing as _t
import datetime as dt

MIN_YYYYMM = os.environ["MIN_YYYYMM"]
OUTPUT_BUCKET = os.environ["OUTPUT_BUCKET"]
OUTPUT_DIR = os.environ["OUTPUT_DIR"]


def fill_missing_jobs() -> _t.Dict[str, str]:
    today = dt.datetime.today()

    # Get all year-month combinations from starting date up to today.
    month_starts = []
    month_start = dt.datetime.strptime(MIN_YYYYMM, "%Y%m")
    while month_start <= today:
        month_starts.append(month_start)

        # Increment to start of next month.
        month_start = month_start + dt.timedelta(days=32)
        month_start = month_start.replace(day=1)

    unique_month_starts = {(ms.year, ms.month) for ms in month_starts}

    # Remove month starts that are already in S3.
    s3_client= boto3.client("s3")
    list_objs_response = s3_client.list_objects_v2(
        Bucket=OUTPUT_BUCKET, 
        Prefix=OUTPUT_DIR
    )
    for obj in list_objs_response["Contents"]:
        yyyymm = re.match(r"bts_airline_ontime_([0-9]{6}).parquet", obj["Key"]).group(1)
        obj_month_start = dt.datetime.strptime(yyyymm, "%Y%m")
        obj_year_month = (obj_month_start.year, obj_month_start.month)
        unique_month_starts.discard(obj_year_month) 

    unique_years = {year for year, _ in unique_month_starts}
    airline_jobs = [
        {"year": f"{year:04}", "month": f"{month:02}"} 
        for year, month in unique_month_starts
    ]
    weather_jobs = [
        {"year": f"{year:04}"}
        for year in unique_years
    ]
    update_station_data = "TRUE"
    update_airport_data = "TRUE"

    return {
        "airline_jobs": airline_jobs,
        "weather_jobs": weather_jobs,
        "update_station_data": update_station_data,
        "update_airport_data": update_airport_data
    }


def historic_jobs() -> _t.Dict[str, str]:
    today = dt.datetime.today()

    # Get all year-month combinations from starting date up to today.
    month_starts = []
    month_start = dt.datetime.strptime(MIN_YYYYMM, "%Y%m")
    while month_start <= today:
        month_starts.append(month_start)

        # Increment to start of next month.
        month_start = month_start + dt.timedelta(days=32)
        month_start = month_start.replace(day=1)

    unique_month_starts = {(ms.year, ms.month) for ms in month_starts}
    unique_years = {ms.year for ms in month_starts}

    airline_jobs = [
        {"year": f"{year:04}", "month": f"{month:02}"} 
        for year, month in unique_month_starts
    ]
    weather_jobs = [
        {"year": f"{year:04}"}
        for year in unique_years
    ]
    update_station_data = "TRUE"
    update_airport_data = "TRUE"

    return {
        "airline_jobs": airline_jobs,
        "weather_jobs": weather_jobs,
        "update_station_data": update_station_data,
        "update_airport_data": update_airport_data
    }
    

def lambda_handler(event, context):
    """ Return the job directives to be sent through a step function to the
    corresponding download scripts. Data pull type is the primary key to
    determine download method and can be one of,
        - FILL_MISSING: Downloads airport data for all dates from the minimum 
        date that do not already have data and updates weather and station data 
        to most recent versions (weather data is by year and station data is 
        time indepedent).

        - HISTORIC: Downloads airport data for all months from the minimum date
        set in the environment and the current date. Downloads relevent weather
        data for time period and updates station data.

    Jobs expected to be in the following format:
        jobs = {
            "airline_jobs": [
                {
                    "year": YYYY, 
                    "month": MM
                }, 
                ...
            ],
            "weather_jobs": [
                {
                    "year": YYYY
                }, 
                ...
            ],
            "update_station_data": TRUE|FALSE
            "update_airport_data": TRUE|FALSE
        }
    """
    
    # Can be FILL_MISSING or HISTORIC.
    data_pull_type = event.get("data_pull_type", None)

    jobs = {}
    if not data_pull_type or data_pull_type == "FILL_MISSING":
        jobs = fill_missing_jobs()
    elif data_pull_type == "HISTORIC":
        jobs = historic_jobs()
    else:
        raise ValueError(
            f"Data pull type {data_pull_type} unrecognized. "
            "Allowed data pull types are FILL_MISSING or HISTORIC.")
    return jobs

