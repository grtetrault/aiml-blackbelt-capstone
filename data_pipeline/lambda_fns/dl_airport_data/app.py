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


def dl_station_data() -> _t.Dict[str, str]:

    # Read in pandas data schema.
    with open(os.path.join(DIRNAME, "pd_schema.json")) as f:
        schema = json.load(f)
    names = [col["name"] for col in schema["fields"]]
    dtype = {i: col["type"] for i, col in enumerate(schema["fields"])}

    # Make request for table data. Data is returned as a fixed-width text file.
    gh_content_fqdn = "https://raw.githubusercontent.com"
    df = pd.read_csv(
        f"{gh_content_fqdn}/jpatokal/openflights/master/data/airports.dat", 
        index_col=None,
        names=names,
        dtype=dtype)

    if len(df) == 0:
        return {
        "status": "NO_DATA"
    }

    # Convert dataframe to parquet.
    data = io.BytesIO()
    df.to_parquet(data, index=False, compression=None)
    data.seek(0)

    # Write to S3 using boto3 to avoid installing fsspec and s3fs. With these
    # packages, the unzipped bundle is too large for Lambda to use.
    s3 = boto3.resource("s3")
    filename = "airports.parquet"
    output_key = os.path.join(OUTPUT_DIR, filename)
    output_bucket_resource = s3.Bucket(OUTPUT_BUCKET)
    output_bucket_resource.upload_fileobj(data, output_key)

    return {
        "status": "SUCCESS",
        "ouput_key": output_key
    }


def lambda_handler(event, context):
    status = dl_station_data()
    return status
