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
    noaa_fqdn = "https://www1.ncdc.noaa.gov"
    response = requests.get(
        f"{noaa_fqdn}/pub/data/ghcn/daily/ghcnd-stations.txt")

    # Infer columns on spacing of all rows. This is not generally recommended,
    # but the file is known to be small enough to fit in memory.
    nrows = response.content.count(b"\n")
    df = pd.read_fwf(
        io.BytesIO(response.content), 
        index_col=False,
        names=names,
        dtype=dtype,
        infer_nrows=nrows)

    # Convert dataframe to parquet.
    data = io.BytesIO()
    df.to_parquet(data, index=False, compression=None)
    data.seek(0)

    # Write to S3 using boto3 to avoid installing fsspec and s3fs. With these
    # packages, the unzipped bundle is too large for Lambda to use.
    s3 = boto3.resource("s3")
    filename = "noaa_stations.parquet"
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
