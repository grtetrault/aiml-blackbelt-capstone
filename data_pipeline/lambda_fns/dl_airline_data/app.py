import os
import io
import json
import boto3
import base64
import zipfile
import requests
import typing as _t

OUTPUT_BUCKET = os.environ["OUTPUT_BUCKET"]
OUTPUT_DIR = os.environ["OUTPUT_DIR"]


def dl_airline_data(year: str, month: str) -> _t.Dict[str, str]:

    s3 = boto3.resource("s3")

    # Prep SQL query for data request.
    query = f"SELECT * FROM t_ontime_reporting WHERE year={year} AND month={month}"
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

    # Unzip data file from downloaded data.
    archive = zipfile.ZipFile(io.BytesIO(response.content))
    data_member_suffix = "_T_ONTIME_REPORTING.csv"
    data_member_name = next(
        n for n in archive.namelist()
        if n.endswith(data_member_suffix))

    # For this dataset, the first row is either column names if specified in
    # the SQL query, or an invalid line if selected all (as we are here).
    header, data = archive.read(data_member_name).split(b"\n", maxsplit=1)

    # Write unzipped data to S3.
    output_key = os.path.join(OUTPUT_DIR, f"bts_airline_ontime_{year}{month}.csv")
    output_bucket_resource = s3.Bucket(OUTPUT_BUCKET)
    output_bucket_resource.upload_fileobj(io.BytesIO(data), output_key)

    return {
        "status": "SUCCESS",
        "ouput_key": output_key,
    }


def lambda_handler(event, context):
    # Note that year and month are both strings.
    year = event["year"]
    month = event["month"]

    status = dl_airline_data(year, month)
    return status
