import os
import aws_cdk as cdk
from dev_env import DevEnvironmentStack
from data_pipeline import DataPipelineStack

app = cdk.App()

data_pipeline_stack = DataPipelineStack(app, 
    "AIML-Blackbelt-DataPipeline"
)

dev_environment_stack = DevEnvironmentStack(app, 
    "AIML-Blackbelt-DevEnvironment",

    dl_pipeline_output_bucket=data_pipeline_stack.dl_output_bucket,

    cluster_name="AIML-Blackbelt-DevCluster",
    cluster_instance_type="m5.xlarge",
    core_instance_count=2,

    notebook_name="AIML-Blackbelt-DevNotebook",
    notebook_instance_type="ml.t3.large",
    notebook_volume_size=10
)

app.synth()