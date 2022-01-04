# -*- coding: utf-8 -*-

import constructs
import aws_cdk as cdk
from aws_cdk import (
    aws_s3 as _s3,
    aws_iam as _iam,
    aws_ec2 as _ec2,
    aws_lambda as _lambda,
    aws_s3_assets as _s3_assets,
    aws_events as _events,
    aws_events_targets as _targets,
    aws_stepfunctions as _sfn,
    aws_stepfunctions_tasks as _tasks)


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
            path="data_pipeline/lambda_fns/dl_airline_data/airline_schema.json"
        )


        # _____________________________________________________________________
        #                                                         Output Bucket
        
        dl_output_bucket = _s3.Bucket(self,
            "DlOutputBucket"
        )


        # _____________________________________________________________________
        #                                                      Lambda Functions

        entrypoint_fn = _lambda.Function(self, 
            "EntryPointFn",
            runtime=_lambda.Runtime.PYTHON_3_7,
            memory_size=512,
            timeout=cdk.Duration.minutes(1),

            handler="app.lambda_handler",
            code=_lambda.Code.from_asset("data_pipeline/lambda_fns/entrypoint/"),
            environment={
                "MIN_YYYYMM": "201101"
            }
        )

        dl_airline_data_fn = _lambda.Function(self, 
            "DlAirlineDataFn",
            runtime=_lambda.Runtime.PYTHON_3_7,
            memory_size= 1769, # Minimum memory that ensures one whole VCPU.
            timeout=cdk.Duration.minutes(15),

            handler="app.lambda_handler",
            code=_lambda.Code.from_asset("data_pipeline/lambda_fns/dl_airline_data/"),
            environment={
                "RETRY_COUNT": "10",
                "RETRY_HEARTBEAT": "1",
                "OUTPUT_BUCKET": dl_output_bucket.bucket_name,
                "OUTPUT_DIR": "dl_output/airline_data"
            }
        )
        dl_output_bucket.grant_read_write(dl_airline_data_fn)

        dl_weather_data_fn = _lambda.Function(self, 
            "DlWeatherDataFn",
            runtime=_lambda.Runtime.PYTHON_3_7,
            memory_size=1024,
            timeout=cdk.Duration.minutes(15),

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
        #                                                         State Machine

        # Wrap Lambda functions in state machine task constructs.
        entrypoint_task = _tasks.LambdaInvoke(self,
            "EntryPointTask",
            lambda_function=entrypoint_fn
        )
        dl_airline_data_task = _tasks.LambdaInvoke(self,
            "DlAirlineDataTask",
            lambda_function=dl_airline_data_fn
        )
        dl_weather_data_task = _tasks.LambdaInvoke(self,
            "DlWeatherDataTask",
            lambda_function=dl_weather_data_fn
        )
        dl_station_data_task = _tasks.LambdaInvoke(self,
            "DlStationDataTask",
            lambda_function=dl_station_data_fn
        )

        # Initiate maps and choice states for executing multiple download 
        # tasks.
        airline_data_map = _sfn.Map(self,
            "AirlineDataMap",
            items_path=_sfn.JsonPath.string_at("$.Payload.airline_jobs"),
            max_concurrency=1
        )
        airline_data_map.iterator(dl_airline_data_task)

        weather_data_map = _sfn.Map(self,
            "WeatherDataMap",
            items_path=_sfn.JsonPath.string_at("$.Payload.weather_jobs"),
            max_concurrency=1
        )
        weather_data_map.iterator(dl_weather_data_task)

        station_data_choice = _sfn.Choice(self,
            "StationDataChoice"
        )
        station_data_choice.when(
            _sfn.Condition.string_equals("$.Payload.update_station_data", "TRUE"),
            dl_station_data_task
        )

        # Execute all downloads in parallel.
        dl_in_parallel = _sfn.Parallel(self, 
            "DlInParallel"
        )
        dl_in_parallel.branch(airline_data_map)
        dl_in_parallel.branch(weather_data_map)
        dl_in_parallel.branch(station_data_choice)

        # Configure state machine.
        state_machine = _sfn.StateMachine(self,
            "StateMachine",
            definition=entrypoint_task.next(dl_in_parallel)
        )

        # Invoke state machine on the last day of every month at 10PM.
        invoke_state_machine_rule = _events.Rule(self,
            "InvokeStateMachineRule",
            schedule=_events.Schedule.expression("cron(0 20 L * ? *)")
        )
        invoke_state_machine_rule.add_target(
            _targets.SfnStateMachine(
                state_machine, 
                input=_events.RuleTargetInput.from_object(
                    {"data_pull_type": "CURRENT_MONTH"}
                )
            )
        )