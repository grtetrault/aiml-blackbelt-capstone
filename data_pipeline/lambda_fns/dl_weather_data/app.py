import os
import io
import json
import boto3
import requests
import typing as _t
import pandas as pd

DIRNAME = os.path.dirname(__file__)
OUTPUT_BUCKET = os.environ["OUTPUT_BUCKET"]
OUTPUT_DIR = os.environ["OUTPUT_DIR"]


def dl_weather_data(year: str) -> _t.Dict[str, str]:

    # Read in pandas data schema.
    with open(os.path.join(DIRNAME, "pd_schema.json")) as f:
        schema = json.load(f)
    names = [col["name"] for col in schema["fields"]]
    dtype = {i: col["type"] for i, col in enumerate(schema["fields"])}

    # Make request for table data. Data is returned as a compressed CSVs.
    noaa_fqdn = "https://www1.ncdc.noaa.gov"
    df = pd.read_csv(
        f"{noaa_fqdn}/pub/data/ghcn/daily/by_year/{year}.csv.gz", 
        compression="gzip",
        index_col=False,
        names=names,
        dtype=dtype)

    # Convert dataframe to parquet.
    data = io.BytesIO()
    df.to_parquet(data, index=False, compression=None)
    data.seek(0)

    # Write to S3 using boto3 to avoid installing fsspec and s3fs. With these
    # packages, the unzipped bundle is too large for Lambda to use.
    s3 = boto3.resource("s3")
    filename = f"noaa_weather_daily_{year}.parquet"
    output_key = os.path.join(OUTPUT_DIR, filename)
    output_bucket_resource = s3.Bucket(OUTPUT_BUCKET)
    output_bucket_resource.upload_fileobj(data, output_key)

    return {
        "status": "SUCCESS",
        "ouput_key": output_key
    }


def lambda_handler(event, context):
    # Note that year and month are both strings.
    year = event["year"]

    status = dl_weather_data(year)
    return status
