# Databricks notebook source
# MAGIC %run "/Repos/Databricks-Notebooks/GCP-DataBricksNotebooks/Production/utility_functions/python_splunk_metrics"

# COMMAND ----------

#%run "/Production/utility_functions/python_splunk_metrics"

# COMMAND ----------

# MAGIC %python
# MAGIC import json
# MAGIC from datetime import datetime
# MAGIC 
# MAGIC feed_name='txn_type_detail'
# MAGIC layer='refined'
# MAGIC system_name='aimia'
# MAGIC execution_time = 0
# MAGIC splunkJobTime = datetime.now()
# MAGIC start_time = datetime.now()
# MAGIC data = []

# COMMAND ----------

prep_folder = "gs://petm-bdpl-prod-prep-p1-gcs-gbl/aimia/"
files = list(filter(lambda x: 'TransactionTypeDetailSync' in x.name, dbutils.fs.ls(prep_folder)))
print('going to process', len(files), 'files')

# COMMAND ----------

try:
  for file in files:
    print("processing : " + file.name)
    start_time = datetime.now()
    # This notebook processes refine.aimia_txn_type_detail_sync
    dbutils.notebook.run('./refine_txn_type_detail', 3600, {"file_name": file.name})
  #  Calculating execution time
    execution_time = execution_time + (datetime.now() - start_time).seconds

  # This notebook creates the AIMIA_TRANSACTION_TYPE parquet 
  if len(files) > 0:
    dbutils.notebook.run('./txn_type_parquet', 3600, {"file_name": file.name})
except Exception as e:
  print(e)
  row = { "layer" : layer, "system_name": system_name, "feed_name": feed_name, "metric_name": "bd_job.failure", "_value": 1 }
  data.append(row)
  raise e
else:
  splunkJobTime =datetime.now()
  total_records = 0;
  files_count = 0;
  for file in files:
    df_aimia_txn_type_detail = spark.sql("""SELECT count(*) as count from refine.aimia_txn_type_detail_sync where trans_detail_file_tstmp =  to_timestamp(regexp_extract('"""+ file.name +"""', '([0-9]{4}[0-9]{2}[0-9]{2}[0-9]{6})', 0), 'yyyyMMddHHmmss') """).head()	

    total_records = total_records + df_aimia_txn_type_detail['count']
    files_count = files_count + 1
    
  print("Total Records inserted::",total_records)
  print("Total Files::",files_count)
  row = { "layer" : layer, "system_name": system_name, "feed_name": feed_name, "metric_name": "bd_job.total_records", "_value": total_records }
  data.append(row) 
  row = { "layer" : layer, "system_name": system_name, "feed_name": feed_name, "metric_name": "bd_job.total_files", "_value": files_count }
  data.append(row)
  row = { "layer" : layer, "system_name": system_name, "feed_name": feed_name, "metric_name": "bd_job.execution_time", "_value": execution_time }
  data.append(row) 
  row = { "layer" : layer, "system_name": system_name, "feed_name": feed_name, "metric_name": "bd_job.success", "_value": 1 }
  data.append(row) 

finally:  
  row = { "layer" : layer, "system_name": system_name, "feed_name": feed_name, "metric_name": "bd_job.count", "_value": 1 }
  data.append(row)
  try:
      SendMetricsJob(data,"prod")
  except:
      print("Error While sending the logs")
  print("Start time of Splunk execution::",splunkJobTime)
  print("End time of Splunk execution::",datetime.now())

# COMMAND ----------

# MAGIC %sql
# MAGIC optimize refine.aimia_txn_type_detail_sync;
