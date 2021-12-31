# -*- coding: utf-8 -*-

import constructs
import aws_cdk as cdk
from aws_cdk import (
    aws_s3 as _s3,
    aws_iam as _iam,
    aws_ec2 as _ec2,
    aws_lambda as _lambda,
    aws_s3_assets as _s3_assets,
    aws_sagemaker as _sagemaker)


class DataPipelineStack(cdk.Stack):
    """ 
    Stack definition of data download pipeline. Currently downloading airport
    cancellation data, daily weather data, and weather station data.
    """

    def __init__(
        self,
        scope: constructs.Construct,
        id: str,
        **kwargs,
    ) -> None:
        super().__init__(scope, id, **kwargs)

        # _____________________________________________________________________
        #                                                                Assets

        airline_schema_asset = _s3_assets.Asset(self, 
            "AirlineSchemaAsset",
            path="data_pipeline/spark_schemas/airline_schema.json"
        )


        # _____________________________________________________________________
        #                                                         Output Bucket
        
        dl_output_bucket = _s3.Bucket(self,
            "DlOutputBucket"
        )


        # _____________________________________________________________________
        #                                                      Lambda Functions

        dl_airline_data_fn = _lambda.Function(self, 
            "DlAirlineDataFn",
            runtime=_lambda.Runtime.PYTHON_3_7,
            memory_size=1024,
            timeout=cdk.Duration.minutes(5),

            handler="app.lambda_handler",
            code=_lambda.Code.from_asset("data_pipeline/lambda_fns/dl_airline_data/"),
            environment={
                "OUTPUT_BUCKET": dl_output_bucket.bucket_name,
                "OUTPUT_DIR": "dl_output/airline_data"
            }
        )
        dl_output_bucket.grant_read_write(dl_airline_data_fn)

        dl_weather_data_fn = _lambda.Function(self, 
            "DlWeatherDataFn",
            runtime=_lambda.Runtime.PYTHON_3_7,
            memory_size=1024,
            timeout=cdk.Duration.minutes(5),

            handler="app.lambda_handler",
            code=_lambda.Code.from_asset("data_pipeline/lambda_fns/dl_weather_data/"),
            environment={
                "OUTPUT_BUCKET": dl_output_bucket.bucket_name,
                "OUTPUT_DIR": "dl_output/weather_data"
            }
        )
        dl_output_bucket.grant_read_write(dl_weather_data_fn)

        dl_station_data_fn = _lambda.Function(self, 
            "DlStationDataFn",
            runtime=_lambda.Runtime.PYTHON_3_7,
            memory_size=1024,
            timeout=cdk.Duration.minutes(5),

            handler="app.lambda_handler",
            code=_lambda.Code.from_asset(
                "data_pipeline/lambda_fns/dl_station_data/",

                # Bundle code to install function requirements.
                bundling=cdk.BundlingOptions(
                    image=_lambda.Runtime.PYTHON_3_7.bundling_image,
                    command=[
                        'bash', '-c',
                        'pip install -r requirements.txt -t /asset-output && rsync -Rru . /asset-output'
                    ],
                )
            ),
            environment={
                "OUTPUT_BUCKET": dl_output_bucket.bucket_name,
                "OUTPUT_DIR": "dl_output/station_data"
            }
        )
        dl_output_bucket.grant_read_write(dl_station_data_fn)


        # _____________________________________________________________________
        #                                                         Step Function