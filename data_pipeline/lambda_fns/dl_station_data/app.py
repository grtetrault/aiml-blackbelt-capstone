import os
import io
import boto3
import requests
import typing as _t
import pandas as pd

OUTPUT_BUCKET = os.environ["OUTPUT_BUCKET"]
OUTPUT_DIR = os.environ["OUTPUT_DIR"]


def dl_station_data() -> _t.Dict[str, str]:

    s3 = boto3.resource("s3")

    # Make request for table data. Data is returned as a fixed-width text file.
    noaa_fqdn = "https://www1.ncdc.noaa.gov"
    response = requests.get(
        f"{noaa_fqdn}/pub/data/ghcn/daily/ghcnd-stations.txt")

    # Infer columns on spacing of all rows. This is not generally recommended,
    # but the file is known to be small.
    nrows = response.content.count(b"\n")
    df = pd.read_fwf(io.BytesIO(response.content), header=None, infer_nrows=nrows)
    data = df.to_csv(header=False, index=False).encode()

    # Write zipped data to S3 (GZIP can be read by Spark).
    output_key = os.path.join(OUTPUT_DIR, f"noaa_stations.csv")
    output_bucket_resource = s3.Bucket(OUTPUT_BUCKET)
    output_bucket_resource.upload_fileobj(io.BytesIO(data), output_key)

    return {
        "status": "SUCCESS",
        "ouput_key": output_key,
    }


def lambda_handler(event, context):
    status = dl_station_data()
    return status
