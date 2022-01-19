# -*- coding: utf-8 -*-
import os.path
import constructs
import aws_cdk as cdk
from aws_cdk import (
    aws_s3 as _s3,
    aws_iam as _iam,
    aws_ec2 as _ec2,
    aws_glue as _glue,
    aws_lambda as _lambda,
    aws_s3_assets as _s3_assets,
    aws_events as _events,
    aws_events_targets as _targets,
    aws_stepfunctions as _sfn,
    aws_stepfunctions_tasks as _tasks)

DIRNAME = os.path.dirname(__file__)


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

        self.etl_script_asset = _s3_assets.Asset(self, 
            "EmrClusterBootstrapAsset",
            path=os.path.join(DIRNAME, "glue_scripts/join_airline_weather_data.py")
        )


        # _____________________________________________________________________
        #                                                               Buckets
        
        self.dl_output_bucket = _s3.Bucket(self,
            "DlOutputBucket"
        )


        # _____________________________________________________________________
        #                                                    Roles and Policies

        self.crawler_role = _iam.Role(self,
            "CrawlerRole",
            assumed_by=_iam.ServicePrincipal("glue.amazonaws.com"),
            managed_policies=[
                _iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSGlueServiceRole"
                ),
                _iam.ManagedPolicy.from_aws_managed_policy_name(
                    "AWSGlueConsoleFullAccess"
                ),
            ]
        )
        self.dl_output_bucket.grant_read_write(self.crawler_role)

        self.etl_job_role = _iam.Role(self,
            "ETLJobRole",
            assumed_by=_iam.ServicePrincipal("glue.amazonaws.com"),
            managed_policies=[
                _iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSGlueServiceRole"
                ),
                _iam.ManagedPolicy.from_aws_managed_policy_name(
                    "AWSGlueConsoleFullAccess"
                ),
            ]
        )
        self.etl_script_asset.grant_read(self.etl_job_role)
        self.dl_output_bucket.grant_read_write(self.etl_job_role)


        # _____________________________________________________________________
        #                                                      Lambda Functions

        self.entrypoint_fn = _lambda.Function(self, 
            "EntryPointFn",
            runtime=_lambda.Runtime.PYTHON_3_7,
            memory_size=512,
            timeout=cdk.Duration.minutes(1),

            handler="app.lambda_handler",
            code=_lambda.Code.from_asset(
                os.path.join(DIRNAME, "lambda_fns/entrypoint/")
            ),
            environment={
                "MIN_YYYYMM": "201101"
            }
        )

        self.dl_airline_data_fn = _lambda.Function(self, 
            "DlAirlineDataFn",
            runtime=_lambda.Runtime.PYTHON_3_7,
            memory_size= 1769, # Minimum memory that ensures one whole VCPU.
            timeout=cdk.Duration.minutes(15),

            handler="app.lambda_handler",
            code=_lambda.Code.from_asset(
                os.path.join(DIRNAME, "lambda_fns/dl_airline_data/"),

                # Bundle code to install function requirements.
                bundling=cdk.BundlingOptions(
                    image=_lambda.Runtime.PYTHON_3_7.bundling_image,
                    command=[
                        "bash", "-c",
                        "pip install -r requirements.txt -t /asset-output && rsync -Rru . /asset-output"
                    ],
                )
            ),
            environment={
                "RETRY_COUNT": "10",
                "RETRY_HEARTBEAT": "1",
                "OUTPUT_BUCKET": self.dl_output_bucket.bucket_name,
                "OUTPUT_DIR": "dl_output/airline_data"
            }
        )
        self.dl_output_bucket.grant_read_write(self.dl_airline_data_fn)

        self.dl_weather_data_fn = _lambda.Function(self, 
            "DlWeatherDataFn",
            runtime=_lambda.Runtime.PYTHON_3_7,
            memory_size=6144,
            timeout=cdk.Duration.minutes(15),

            handler="app.lambda_handler",
            code=_lambda.Code.from_asset(
                os.path.join(DIRNAME, "lambda_fns/dl_weather_data/"),

                # Bundle code to install function requirements.
                bundling=cdk.BundlingOptions(
                    image=_lambda.Runtime.PYTHON_3_7.bundling_image,
                    command=[
                        "bash", "-c",
                        "pip install -r requirements.txt -t /asset-output && rsync -Rru . /asset-output"
                    ],
                )
            ),
            environment={
                "OUTPUT_BUCKET": self.dl_output_bucket.bucket_name,
                "OUTPUT_DIR": "dl_output/weather_data"
            }
        )
        self.dl_output_bucket.grant_read_write(self.dl_weather_data_fn)

        self.dl_station_data_fn = _lambda.Function(self, 
            "DlStationDataFn",
            runtime=_lambda.Runtime.PYTHON_3_7,
            memory_size=1769, # Minimum memory that ensures one whole VCPU.
            timeout=cdk.Duration.minutes(5),

            handler="app.lambda_handler",
            code=_lambda.Code.from_asset(
                os.path.join(DIRNAME, "lambda_fns/dl_station_data/"),

                # Bundle code to install function requirements.
                bundling=cdk.BundlingOptions(
                    image=_lambda.Runtime.PYTHON_3_7.bundling_image,
                    command=[
                        "bash", "-c",
                        "pip install -r requirements.txt -t /asset-output && rsync -Rru . /asset-output"
                    ],
                )
            ),
            environment={
                "OUTPUT_BUCKET": self.dl_output_bucket.bucket_name,
                "OUTPUT_DIR": "dl_output/station_data"
            }
        )
        self.dl_output_bucket.grant_read_write(self.dl_station_data_fn)

        self.dl_airport_data_fn = _lambda.Function(self, 
            "DlAirportDataFn",
            runtime=_lambda.Runtime.PYTHON_3_7,
            memory_size=1769, # Minimum memory that ensures one whole VCPU.
            timeout=cdk.Duration.minutes(5),

            handler="app.lambda_handler",
            code=_lambda.Code.from_asset(
                os.path.join(DIRNAME, "lambda_fns/dl_airport_data/"),

                # Bundle code to install function requirements.
                bundling=cdk.BundlingOptions(
                    image=_lambda.Runtime.PYTHON_3_7.bundling_image,
                    command=[
                        "bash", "-c",
                        "pip install -r requirements.txt -t /asset-output && rsync -Rru . /asset-output"
                    ],
                )
            ),
            environment={
                "OUTPUT_BUCKET": self.dl_output_bucket.bucket_name,
                "OUTPUT_DIR": "dl_output/airport_data"
            }
        )
        self.dl_output_bucket.grant_read_write(self.dl_airport_data_fn)


        # _____________________________________________________________________
        #                                                        Glue Resources

        self.database = _glue.CfnDatabase(self,
            "Database",
            catalog_id=cdk.Stack.of(self).account,
            database_input=_glue.CfnDatabase.DatabaseInputProperty(
                description="Glue DB for AIML Blackbelt data pipeline output"
            )
        )

        self.crawler = _glue.CfnCrawler(self,
            "Crawler",
            role=self.crawler_role.role_name,
            database_name=self.database.ref,
            targets=_glue.CfnCrawler.TargetsProperty(
                s3_targets=[
                    _glue.CfnCrawler.S3TargetProperty(
                        path=f"s3://{self.dl_output_bucket.bucket_name}/dl_output"
                    )
                ]
            ),
            configuration="{ \"Version\": 1.0 }"
        )

        self.etl_job = _glue.CfnJob(self,
            "ETLJob",
            glue_version="2.0",
            max_capacity=20,
            role=self.etl_job_role.role_arn,
            command=_glue.CfnJob.JobCommandProperty(
                name="glueetl",
                python_version="3",
                script_location=self.etl_script_asset.s3_object_url
            ),
            default_arguments={
                "--dl_output_glue_db": self.database.ref, 
                "--etl_output_bucket": self.dl_output_bucket.bucket_name
            },
            timeout=(60 * 24) # minutes.
        )

        # _____________________________________________________________________
        #                                                                 Tasks

        # Wrap Lambda functions in state machine task constructs.
        self.entrypoint_task = _tasks.LambdaInvoke(self,
            "EntryPointTask",
            lambda_function=self.entrypoint_fn
        )
        self.dl_airline_data_task = _tasks.LambdaInvoke(self,
            "DlAirlineDataTask",
            lambda_function=self.dl_airline_data_fn
        )
        self.dl_weather_data_task = _tasks.LambdaInvoke(self,
            "DlWeatherDataTask",
            lambda_function=self.dl_weather_data_fn
        )
        self.dl_station_data_task = _tasks.LambdaInvoke(self,
            "DlStationDataTask",
            lambda_function=self.dl_station_data_fn
        )
        self.dl_airport_data_task = _tasks.LambdaInvoke(self,
            "DlAirportDataTask",
            lambda_function=self.dl_airport_data_fn
        )

        # Task for calling Glue resources. Note that crawlers cannot be called
        # synchronously, so a heartbeay loop checking status must be 
        # implemented.
        self.start_crawler_task = _tasks.CallAwsService(self,
            "StartCrawlerTask",
            service="glue",
            action="startCrawler",
            parameters={
                "Name": self.crawler.ref
            },
            iam_resources=[
                f"arn:aws:glue:{cdk.Stack.of(self).region}:{cdk.Stack.of(self).account}:*"
            ]
        )
        self.get_crawler_status_task = _tasks.CallAwsService(self,
            "GetCrawlerStatusTask",
            service="glue",
            action="getCrawler",
            parameters={
                "Name": self.crawler.ref
            },
            iam_resources=[
                f"arn:aws:glue:{cdk.Stack.of(self).region}:{cdk.Stack.of(self).account}:*"
            ]
        )
        self.etl_job_sync_task = _tasks.GlueStartJobRun(self,
            "ETLJobSyncTask",
            glue_job_name=self.etl_job.ref,
            heartbeat=cdk.Duration.minutes(5)
        )


        # _____________________________________________________________________
        #                                                         State Machine

        # Configure state machine.
        self.state_machine = _sfn.StateMachine(self,
            "StateMachine",
            definition=(
                self.entrypoint_task
                .next(
                    _sfn.Parallel(self, "DlInParallel")
                    .branch(
                        _sfn.Map(self, "AirlineDataMap",
                            items_path=_sfn.JsonPath.string_at("$.Payload.airline_jobs"),
                            max_concurrency=4 # Concurrency reduced to respect BTS request restraints.
                        )
                        .iterator(self.dl_airline_data_task)
                    )
                    .branch(
                        _sfn.Map(self, "WeatherDataMap",
                            items_path=_sfn.JsonPath.string_at("$.Payload.weather_jobs")
                        )
                        .iterator(self.dl_weather_data_task)
                    )
                    .branch(
                        _sfn.Choice(self, "StationDataChoice")
                        .when(
                            _sfn.Condition.string_equals("$.Payload.update_station_data", "TRUE"),
                            self.dl_station_data_task
                        )
                    )
                    .branch(
                        _sfn.Choice(self, "AirportDataChoice")
                        .when(
                            _sfn.Condition.string_equals("$.Payload.update_airport_data", "TRUE"),
                            self.dl_airport_data_task
                        )
                    )
                )
                .next(self.start_crawler_task)
                .next(self.get_crawler_status_task)
                .next(
                    _sfn.Choice(self, "CrawlerStatusChoice")
                    .when(
                        condition=_sfn.Condition.string_equals("$.Crawler.State", "RUNNING"),
                        next=_sfn.Wait(self, "CrawlerStatusWait",
                            time=_sfn.WaitTime.duration(cdk.Duration.seconds(30))
                        ).next(self.get_crawler_status_task)
                    )
                    .otherwise(self.etl_job_sync_task)
                )
            )
        )

        # Invoke state machine on the last day of every month at 10PM.
        self.invoke_state_machine_rule = _events.Rule(self,
            "InvokeStateMachineRule",
            schedule=_events.Schedule.expression("cron(0 20 L * ? *)")
        )
        self.invoke_state_machine_rule.add_target(
            _targets.SfnStateMachine(
                self.state_machine, 
                input=_events.RuleTargetInput.from_object(
                    {"data_pull_type": "CURRENT_MONTH"}
                )
            )
        )