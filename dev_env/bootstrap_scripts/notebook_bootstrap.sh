#!/bin/bash
# Add environment variables to conda environment.
touch /home/ec2-user/anaconda3/envs/python3/etc/conda/activate.d/env_vars.sh
echo "export DATA_BUCKET='${DATA_BUCKET}'" >> /home/ec2-user/anaconda3/envs/python3/etc/conda/activate.d/env_vars.sh
echo "export CLUSTER_ID='${CLUSTER_ID}'" >> /home/ec2-user/anaconda3/envs/python3/etc/conda/activate.d/env_vars.sh

# Required to use widgets with Jupyter.
pip install ipywidgets
jupyter nbextension enable --py widgetsnbextension
jupyter labextension install @jupyter-widgets/jupyterlab-manager

# As the notebook depends on the cluster being created successfully in the
# stack, we assume below that the cluster nodes have been provisioned.

# Find the master node instance (assumes that there is only one).
cluster_state=$(aws emr describe-cluster \
    --cluster-id $CLUSTER_ID             \
    --query "Cluster.Status.State"       \
    --output text)
echo Cluster ${CLUSTER_ID} state: ${cluster_state}

# Get master node private IP and configure Sparkmagic/Livy to use it.
master_ip_address=$(aws emr list-instances  \
    --cluster-id $CLUSTER_ID                \
    --instance-group-types MASTER           \
    --query "Instances[0].PrivateIpAddress" \
    --output text)
echo Master node private IP: $master_ip_address

cat > /home/ec2-user/.sparkmagic/config.json << EOL
{
  "kernel_python_credentials" : {
    "username": "",
    "password": "",
    "url": "http://${master_ip_address}:8998",
    "auth": "None"
  },

  "kernel_scala_credentials" : {
    "username": "",
    "password": "",
    "url": "http://${master_ip_address}:8998",
    "auth": "None"
  },
  "kernel_r_credentials": {
    "username": "",
    "password": "",
    "url": "http://${master_ip_address}:8998"
  },

  "session_configs": {
    "name":"remotesparkmagics-notebook", 
    "kind": "pyspark3",
    "conf": {
      "spark.pyspark.python": "python3",
      "spark.jars.packages": "ml.combust.mleap:mleap-spark_2.11:0.17.0"
    }
  },

  "logging_config": {
    "version": 1,
    "formatters": {
      "magicsFormatter": { 
        "format": "%(asctime)s\t%(levelname)s\t%(message)s",
        "datefmt": ""
      }
    },
    "handlers": {
      "magicsHandler": { 
        "class": "hdijupyterutils.filehandler.MagicsFileHandler",
        "formatter": "magicsFormatter",
        "home_path": "~/.sparkmagic"
      }
    },
    "loggers": {
      "magicsLogger": { 
        "handlers": ["magicsHandler"],
        "level": "DEBUG",
        "propagate": 0
      }
    }
  },
  "authenticators": {
    "Kerberos": "sparkmagic.auth.kerberos.Kerberos",
    "None": "sparkmagic.auth.customauth.Authenticator", 
    "Basic_Access": "sparkmagic.auth.basic.Basic"
  },

  "wait_for_idle_timeout_seconds": 15,
  "livy_session_startup_timeout_seconds": 60,

  "fatal_error_suggestion": "The code failed because of a fatal error:\n\t{}.\n\nSome things to try:\na) Make sure Spark has enough available resources for Jupyter to create a Spark context.\nb) Contact your Jupyter administrator to make sure the Spark magics library is configured correctly.\nc) Restart the kernel.",

  "ignore_ssl_errors": false,

  "use_auto_viz": true,
  "coerce_dataframe": true,
  "max_results_sql": 2500,
  "pyspark_dataframe_encoding": "utf-8",
  
  "heartbeat_refresh_seconds": 30,
  "livy_server_heartbeat_timeout_seconds": 0,
  "heartbeat_retry_seconds": 10,

  "server_extension_default_kernel_name": "pysparkkernel",
  "custom_headers": {},
  
  "retry_policy": "configurable",
  "retry_seconds_to_sleep_list": [0.2, 0.5, 1, 3, 5],
  "configurable_retry_policy_max_retries": 8
}
EOL