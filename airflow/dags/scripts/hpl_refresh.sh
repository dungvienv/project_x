#!/bin/bash

# Remote SSH connection
ssh_username="dung.vienv"
remote_host="bdp-edge01-pdc.vn.prod"

# Remote Spark script path
remote_script_path="scripts/flow_last_30d_lead_hpl.py"

# parquet file name
parquet_filename="hpl_lead.parquet"

# Local destination path
local_destination="$AIRFLOW_HOME/hpl_lead_data"

# Temporary directory on remote server
remote_temp_dir="./temp"

# Step 0: Clear existing files and folders
if [ -d "$local_destination" ]; then
    rm -r "${local_destination}"
else
    mkdir -p "${local_destination}"    
fi   
echo "$PWD"
ssh "${ssh_username}@${remote_host}" "rm -rf ${remote_temp_dir}"
ssh "${ssh_username}@${remote_host}" "mkdir -p ${remote_temp_dir}/${parquet_filename}"

# Step 1: Run Spark script remotely to generate parquet file on HDFS
ssh "${ssh_username}@${remote_host}" "spark-submit ${remote_script_path}"

# Step 2: Copy parquet file from HDFS to remote server
ssh "${ssh_username}@${remote_host}" "hadoop fs -copyToLocal ./temp/${parquet_filename} ${remote_temp_dir}/"

# Step 3: Copy parquet file from remote server to local machine
scp -r "${ssh_username}@${remote_host}:${remote_temp_dir}/${parquet_filename}" "${local_destination}"

# Step 4: Clean up temporary files on the remote server
# ssh "${ssh_username}@${remote_host}" "rm -rf ${remote_temp_dir}"
