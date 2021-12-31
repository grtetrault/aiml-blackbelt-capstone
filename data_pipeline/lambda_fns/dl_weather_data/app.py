import os
import io
import boto3
import requests
import typing as _t

OUTPUT_BUCKET = os.environ["OUTPUT_BUCKET"]
OUTPUT_DIR = os.environ["OUTPUT_DIR"]


def dl_weather_data(year: str) -> _t.Dict[str, str]:

    s3 = boto3.resource("s3")

    # Make request for table data. Data is returned as a compressed CSVs.
    noaa_fqdn = "https://www1.ncdc.noaa.gov"
    response = requests.get(
        f"{noaa_fqdn}/pub/data/ghcn/daily/by_year/{year}.csv.gz")

    # Write zipped data to S3 (GZIP can be read by Spark).
    output_key = os.path.join(OUTPUT_DIR, f"noaa_weather_daily_{year}.csv.gz")
    output_bucket_resource = s3.Bucket(OUTPUT_BUCKET)
    output_bucket_resource.upload_fileobj(io.BytesIO(response.content), output_key)

    return {
        "status": "SUCCESS",
        "ouput_key": output_key,
    }


def lambda_handler(event, context):
    # Note that year and month are both strings.
    year = event["year"]

    status = dl_weather_data(year)
    return status
