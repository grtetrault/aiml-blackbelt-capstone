# -*- coding: utf-8 -*-

import constructs
import aws_cdk as cdk
from aws_cdk import (
    aws_s3 as _s3,
    aws_iam as _iam,
    aws_ec2 as _ec2,
    aws_emr as _emr, 
    aws_s3_assets as _s3_assets,
    aws_sagemaker as _sagemaker)


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
        cluster_name: str, 
        cluster_instance_type: str,
        core_instance_count: int,
        # s3_log_bucket: _s3.Bucket,

        notebook_name: str,
        notebook_instance_type: str,
        notebook_volume_size: int,

        **kwargs,
    ) -> None:
        super().__init__(scope, id, **kwargs)

        # _____________________________________________________________________
        #                                                                Assets

        cluster_bootstrap_asset = _s3_assets.Asset(self, 
            "EmrClusterBootstrapAsset",
            path="dev_env/bootstrap_scripts/emr_bootstrap.sh"
        )

        notebook_bootstrap_asset = _s3_assets.Asset(self, 
            "NotebookBootstrapAsset",
            path="dev_env/bootstrap_scripts/notebook_bootstrap.sh"
        )

        # _____________________________________________________________________
        #                                                        VPC and Subnet
        
        vpc = _ec2.Vpc(self, 
          "Vpc",
            nat_gateways=0,
            subnet_configuration=[
                _ec2.SubnetConfiguration(
                    name="public", subnet_type=_ec2.SubnetType.PUBLIC
                )
            ]
        )

        subnet = vpc.public_subnets[0]


        # _____________________________________________________________________
        #                                                       Security Groups

        # Security group to allow Livy interaction between cluster and notebook.
        additional_master_sg = _ec2.SecurityGroup(self, 
            "EmrAdditionalMasterSg",
            vpc=vpc
        )

        notebook_sg = _ec2.SecurityGroup(self, 
            "NotebookSg",
            vpc=vpc,
            allow_all_outbound=True
        )

        # Inbound security group rules.
        additional_master_sg.add_ingress_rule(
            peer=notebook_sg,
            connection=_ec2.Port.tcp(8998),
            description="Livy Port"
        )

        notebook_sg.add_ingress_rule(
            peer=_ec2.Peer.any_ipv4(), 
            connection=_ec2.Port.tcp(80)
        )
        notebook_sg.add_ingress_rule(
            peer=_ec2.Peer.any_ipv4(),
            connection=_ec2.Port.tcp(443)
        )
        notebook_sg.add_ingress_rule(
            peer=_ec2.Peer.any_ipv6(),
            connection=_ec2.Port.tcp(80)
        )
        notebook_sg.add_ingress_rule(
            peer=_ec2.Peer.any_ipv6(),
            connection=_ec2.Port.tcp(443)
        )

        # _____________________________________________________________________
        #                                                    Roles and Policies

        # TODO: Update EMR policy to V2 and restrict S3 permissions to data bucket.
        # Roles for cluster and isntances.
        emr_service_role = _iam.Role(self,
            "EmrServiceRole",
            assumed_by=_iam.ServicePrincipal("elasticmapreduce.amazonaws.com"),
            managed_policies=[
                _iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AmazonElasticMapReduceRole"
                )
            ]
        )
        emr_job_flow_role = _iam.Role(self,
            "EmrJobFlowRole",
            assumed_by=_iam.ServicePrincipal("ec2.amazonaws.com"),
            managed_policies=[
                _iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AmazonElasticMapReduceforEC2Role"
                ),
                _iam.ManagedPolicy.from_aws_managed_policy_name(
                    "AmazonSageMakerFullAccess"
                ),
            ]
        )
        emr_job_flow_profile = _iam.CfnInstanceProfile(self,
            "EmrJobFlowProfile",
            roles=[emr_job_flow_role.role_name]
        )

        notebook_service_role = _iam.Role(self, 
            "NotebookServiceRole",
            assumed_by=_iam.ServicePrincipal("sagemaker.amazonaws.com"),
            managed_policies=[
                _iam.ManagedPolicy.from_aws_managed_policy_name(
                    "AmazonSageMakerFullAccess"
                ),
                # Required for script to find master node IP.
                _iam.ManagedPolicy.from_aws_managed_policy_name(
                    "AmazonEMRReadOnlyAccessPolicy_v2"
                )
            ]
        )
        notebook_bootstrap_asset.grant_read(notebook_service_role)


        # _____________________________________________________________________
        #                                                           EMR Cluster

        cluster = _emr.CfnCluster(self, 
            "EmrCluster",
            name=cluster_name,
            release_label="emr-6.4.0",
            visible_to_all_users=False,

            # Note job_flow_role is an instance profile (not an IAM role).
            job_flow_role=emr_job_flow_profile.ref,
            service_role=emr_service_role.role_name,

            #log_uri=f"s3://{s3_log_bucket.bucket_name}/{cdk.Aws.REGION}/elasticmapreduce/",

            instances=_emr.CfnCluster.JobFlowInstancesConfigProperty(
                hadoop_version="Amazon",
                ec2_subnet_id=subnet.subnet_id,
                keep_job_flow_alive_when_no_steps=True,
                additional_master_security_groups=[additional_master_sg.security_group_id],
                
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
                _emr.CfnCluster.ApplicationProperty(name="Livy")
            ],

            bootstrap_actions=[
                _emr.CfnCluster.BootstrapActionConfigProperty(
                    name="cluster-bootstrap",
                    script_bootstrap_action=_emr.CfnCluster.ScriptBootstrapActionConfigProperty(
                        path=cluster_bootstrap_asset.s3_object_url
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
        notebook_lifecycle_config = _sagemaker.CfnNotebookInstanceLifecycleConfig(self,
            "NotebookLifecycleConfig",
            on_create=[
                _sagemaker.CfnNotebookInstanceLifecycleConfig.NotebookInstanceLifecycleHookProperty(
                    content=cdk.Fn.base64(
                        "#!/bin/bash\n"
                        f"export CLUSTER_ID={cluster.ref}\n"
                        f"aws s3 cp {notebook_bootstrap_asset.s3_object_url} ./bootstrap.sh\n"
                        "nohup bash bootstrap.sh\n"
                        "rm bootstrap.sh"
                    )
                )
            ]
        )
                
        notebook = _sagemaker.CfnNotebookInstance(self, 
            "NotebookInstance",
            notebook_instance_name=notebook_name,
            role_arn=notebook_service_role.role_arn,
            lifecycle_config_name=notebook_lifecycle_config.get_att("NotebookInstanceLifecycleConfigName").to_string(),

            # default_code_repository=code_repository,
            security_group_ids=[notebook_sg.security_group_id],
            subnet_id=subnet.subnet_id,

            instance_type=notebook_instance_type,
            platform_identifier="notebook-al2-v1",
            volume_size_in_gb=notebook_volume_size
        )
        notebook.node.add_dependency(cluster)
