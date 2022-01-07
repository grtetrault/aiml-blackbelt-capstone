# -*- coding: utf-8 -*-
import os.path
import constructs
import aws_cdk as cdk
from aws_cdk import (
    aws_s3 as _s3,
    aws_iam as _iam,
    aws_ec2 as _ec2,
    aws_emr as _emr, 
    aws_s3_assets as _s3_assets,
    aws_sagemaker as _sagemaker)

DIRNAME = os.path.dirname(__file__)


class DevEnvironmentStack(cdk.Stack):
    """ 
    Class definition a development environment consisting of:
        - A Spark cluster with Python data science packages installed and,
        - A SageMaker notebook isntance that can interact with the Spark 
          cluster through the PySpark kernel.
              
    Note that stack requires manual deletion of VPC and the cluster's 
    auto-generated security groups. These security groups are not managed by
    the stack and hence cannot be automatically deleted.
    """

    def __init__(
        self,
        scope: constructs.Construct,
        id: str,

        dl_pipeline_output_bucket: _s3.Bucket,

        cluster_name: str, 
        cluster_instance_type: str,
        core_instance_count: int,

        notebook_name: str,
        notebook_instance_type: str,
        notebook_volume_size: int,

        **kwargs,
    ) -> None:
        super().__init__(scope, id, **kwargs)

        # _____________________________________________________________________
        #                                                                Assets

        self.cluster_bootstrap_asset = _s3_assets.Asset(self, 
            "EmrClusterBootstrapAsset",
            path=os.path.join(DIRNAME, "bootstrap_scripts/emr_bootstrap.sh")
        )

        self.notebook_bootstrap_asset = _s3_assets.Asset(self,
            "NotebookBootstrapAsset",
            path=os.path.join(DIRNAME, "bootstrap_scripts/notebook_bootstrap.sh")
        )


        # _____________________________________________________________________
        #                                                               Buckets
        
        self.model_bucket = _s3.Bucket(self,
            "ModelBucket"
        )

        self.emr_log_bucket = _s3.Bucket(self,
            "EMRLogBucket",
            auto_delete_objects=True,
            removal_policy=cdk.RemovalPolicy.DESTROY
        )


        # _____________________________________________________________________
        #                                                        VPC and Subnet
        
        self.vpc = _ec2.Vpc(self, 
            "Vpc",
            cidr="10.0.0.0/16",
            nat_gateways=0
        )

        self.subnet = _ec2.PublicSubnet(self,
            "Subnet",
            availability_zone=cdk.Stack.of(self).availability_zones[0],
            cidr_block="10.0.0.0/16",
            vpc_id=self.vpc.vpc_id
        )

        # _____________________________________________________________________
        #                                                       Security Groups

        # Security group to allow Livy interaction between cluster and notebook.
        self.additional_master_sg = _ec2.SecurityGroup(self, 
            "EmrAdditionalMasterSg",
            vpc=self.vpc
        )

        self.notebook_sg = _ec2.SecurityGroup(self, 
            "NotebookSg",
            vpc=self.vpc,
            allow_all_outbound=True
        )

        # Inbound security group rules.
        self.additional_master_sg.add_ingress_rule(
            peer=self.notebook_sg,
            connection=_ec2.Port.tcp(8998),
            description="Livy Port"
        )

        self.notebook_sg.add_ingress_rule(
            peer=_ec2.Peer.any_ipv4(), 
            connection=_ec2.Port.tcp(80)
        )
        self.notebook_sg.add_ingress_rule(
            peer=_ec2.Peer.any_ipv4(),
            connection=_ec2.Port.tcp(443)
        )
        self.notebook_sg.add_ingress_rule(
            peer=_ec2.Peer.any_ipv6(),
            connection=_ec2.Port.tcp(80)
        )
        self.notebook_sg.add_ingress_rule(
            peer=_ec2.Peer.any_ipv6(),
            connection=_ec2.Port.tcp(443)
        )

        # _____________________________________________________________________
        #                                                    Roles and Policies

        # TODO: Update EMR policy to V2 and restrict S3 permissions to data bucket.
        # Roles for cluster and isntances.
        self.emr_service_role = _iam.Role(self,
            "EmrServiceRole",
            assumed_by=_iam.ServicePrincipal("elasticmapreduce.amazonaws.com"),
            managed_policies=[
                _iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AmazonEMRServicePolicy_v2"
                )
            ]
        )
        self.emr_job_flow_role = _iam.Role(self,
            "EmrJobFlowRole",
            assumed_by=_iam.ServicePrincipal("ec2.amazonaws.com"),
            managed_policies=[
                _iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AmazonElasticMapReduceforEC2Role"
                )
            ]
        )
        self.emr_log_bucket.grant_read_write(self.emr_job_flow_role)
        dl_pipeline_output_bucket.grant_read_write(self.emr_job_flow_role)
        # Allow read write access to data bucket and to assets (schemas).

        self.emr_job_flow_profile = _iam.CfnInstanceProfile(self,
            "EmrJobFlowProfile",
            roles=[self.emr_job_flow_role.role_name]
        )

        self.notebook_service_role = _iam.Role(self, 
            "NotebookServiceRole",
            assumed_by=_iam.ServicePrincipal("sagemaker.amazonaws.com"),
            managed_policies=[
                _iam.ManagedPolicy.from_aws_managed_policy_name(
                    "AmazonSageMakerFullAccess"
                ),
                # Required for script to find master node IP.
                _iam.ManagedPolicy.from_aws_managed_policy_name(
                    "AmazonEMRReadOnlyAccessPolicy_v2"
                ),
                # Required to find data locations outputted by data pipeline.
                _iam.ManagedPolicy.from_aws_managed_policy_name(
                    "AWSCloudFormationReadOnlyAccess"
                )
            ]
        )
        self.notebook_bootstrap_asset.grant_read(self.notebook_service_role)
        self.model_bucket.grant_read_write(self.notebook_service_role)


        # _____________________________________________________________________
        #                                                           EMR Cluster

        self.cluster = _emr.CfnCluster(self, 
            "EmrCluster",
            name=cluster_name,
            release_label="emr-5.30.2",
            visible_to_all_users=False,

            # Note job_flow_role is an instance profile (not an IAM role).
            job_flow_role=self.emr_job_flow_profile.ref,
            service_role=self.emr_service_role.role_name,

            log_uri=f"s3://{self.emr_log_bucket.bucket_name}/{cdk.Aws.REGION}/elasticmapreduce/",

            instances=_emr.CfnCluster.JobFlowInstancesConfigProperty(
                hadoop_version="Amazon",
                ec2_subnet_id=self.subnet.subnet_id,
                keep_job_flow_alive_when_no_steps=True,
                additional_master_security_groups=[self.additional_master_sg.security_group_id],
                
                core_instance_group=_emr.CfnCluster.InstanceGroupConfigProperty(
                    instance_count=core_instance_count, 
                    instance_type=cluster_instance_type, 
                    market="ON_DEMAND"
                ),
                master_instance_group=_emr.CfnCluster.InstanceGroupConfigProperty(
                    instance_count=1, 
                    instance_type=cluster_instance_type, 
                    market="ON_DEMAND"
                )
            ),

            applications=[
                _emr.CfnCluster.ApplicationProperty(name="Spark"),
                _emr.CfnCluster.ApplicationProperty(name="Livy"),
                _emr.CfnCluster.ApplicationProperty(name="Hive")
            ],

            bootstrap_actions=[
                _emr.CfnCluster.BootstrapActionConfigProperty(
                    name="cluster-bootstrap",
                    script_bootstrap_action=_emr.CfnCluster.ScriptBootstrapActionConfigProperty(
                        path=self.cluster_bootstrap_asset.s3_object_url
                    )
                )
            ],

            configurations=[
                # Use python3 for pyspark.
                _emr.CfnCluster.ConfigurationProperty(
                    classification="spark-env",
                    configurations=[
                        _emr.CfnCluster.ConfigurationProperty(
                            classification="export",
                            configuration_properties={
                                "PYSPARK_PYTHON": "/usr/bin/python3",
                                "PYSPARK_DRIVER_PYTHON": "/usr/bin/python3"
                            }
                        )
                    ]
                ),
                # Enable apache arrow.
                _emr.CfnCluster.ConfigurationProperty(
                    classification="spark-defaults",
                    configuration_properties={
                        "spark.sql.execution.arrow.enabled": "true"
                    }
                ),
                # Dedicate cluster to single jobs.
                _emr.CfnCluster.ConfigurationProperty(
                    classification="spark",
                    configuration_properties={
                        "maximizeResourceAllocation": "true"
                    }
                ),
            ]
        )


        #______________________________________________________________________
        #                                           Sagemaker Notebook Instance

        # code_repository = _sagemaker.CfnCodeRepository(self, "CodeRepository",
        #     code_repository_name=f"{sm_notebook_name}_repository",
        #     git_config=_sagemaker.CfnCodeRepository.GitConfigProperty(
        #         repository_url="repositoryUrl", # TODO enter git repository.
        #     )
        # )

        # Configure notebook instance bootstrap script.
        self.notebook_lifecycle_config = _sagemaker.CfnNotebookInstanceLifecycleConfig(self,
            "NotebookLifecycleConfig",
            on_create=[
                _sagemaker.CfnNotebookInstanceLifecycleConfig.NotebookInstanceLifecycleHookProperty(
                    content=cdk.Fn.base64(
                        "#!/bin/bash\n"
                        f"export DATA_BUCKET={dl_pipeline_output_bucket.bucket_name}\n"
                        f"export CLUSTER_ID={self.cluster.ref}\n"
                        f"aws s3 cp {self.notebook_bootstrap_asset.s3_object_url} ./bootstrap.sh\n"
                        "bash bootstrap.sh\n"
                        "rm bootstrap.sh"
                    )
                )
            ]
        )
                
        notebook = _sagemaker.CfnNotebookInstance(self, 
            "NotebookInstance",
            notebook_instance_name=notebook_name,
            role_arn=self.notebook_service_role.role_arn,
            lifecycle_config_name=self.notebook_lifecycle_config.get_att("NotebookInstanceLifecycleConfigName").to_string(),

            # default_code_repository=code_repository,
            security_group_ids=[self.notebook_sg.security_group_id],
            subnet_id=self.subnet.subnet_id,

            instance_type=notebook_instance_type,
            platform_identifier="notebook-al2-v1",
            volume_size_in_gb=notebook_volume_size
        )
        notebook.node.add_dependency(self.cluster)
