import os
import io
import json
import boto3
import base64
import requests
import typing as _t
import pandas as pd

DIRNAME = os.path.dirname(__file__)
OUTPUT_BUCKET = os.environ["OUTPUT_BUCKET"]
OUTPUT_DIR = os.environ["OUTPUT_DIR"]

RETRY_COUNT = int(os.environ["RETRY_COUNT"])
RETRY_HEARTBEAT = int(os.environ["RETRY_HEARTBEAT"])


def dl_airline_data(year: str, month: str) -> _t.Dict[str, str]:

    # Read in pandas data schema.
    with open(os.path.join(DIRNAME, "pd_schema.json")) as f:
        schema = json.load(f)
    names = [col["name"] for col in schema["fields"]]
    dtype = {i: col["type"] for i, col in enumerate(schema["fields"])}

    # Prep SQL query for data request.
    query = f"SELECT {','.join(names)} FROM t_ontime_reporting WHERE year={year} AND month={month}"
    encoded_query = base64.b64encode(query.encode()).decode("utf-8")

    # Make request for table data. Data is returned as a zip file.
    bts_fqdn = "https://www.transtats.bts.gov"
    bts_request_params = {
        "gnoyr_Vq": "FGJ",
        "Un5_T4172": "G",
        "V5_mv22rq": "D"
    }
    bts_request_data = {
        "UserTableName": "Reporting_Carrier_On_Time_Performance_1987_present",
        "DBShortName": "On_Time",
        "RawDataTable": "T_ONTIME_REPORTING",
        "sqlstr": encoded_query
    }
    response = requests.post(
        f"{bts_fqdn}/DownLoad_Table.asp",
        params=bts_request_params,
        data=bts_request_data,
        verify=False)

    # Note that column names are set in query sent in the request above.
    df = pd.read_csv(
        io.BytesIO(response.content),
        compression="zip",
        index_col=False,
        usecols=names,
        dtype=dtype)

    # Convert dataframe to parquet.
    data = io.BytesIO()
    df.to_parquet(data, index=False, compression=None)
    data.seek(0)

    # Write to S3 using boto3 to avoid installing fsspec and s3fs. With these
    # packages, the unzipped bundle is too large for Lambda to use.
    # TODO: Look into smaller bundling methods to include these packages.
    s3 = boto3.resource("s3")
    filename = f"bts_airline_ontime_{year}{month}.parquet"
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
    month = event["month"]

    status = dl_airline_data(year, month)
    return status
