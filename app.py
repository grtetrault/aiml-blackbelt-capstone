import os
import aws_cdk as cdk
from dev_env import DevEnvironmentStack

app = cdk.App()
DevEnvironmentStack(app, "DevEnvironmentStack",
    cluster_name="AIML-Blackbelt-DevCluster",
    cluster_instance_type="m5.xlarge",
    core_instance_count=2,

    notebook_name="AIML-Blackbelt-DevNotebook",
    notebook_instance_type="ml.t3.large",
    notebook_volume_size=10
)

app.synth()