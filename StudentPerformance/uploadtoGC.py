# Databricks notebook source


from pyspark.sql.functions import expr, coalesce, when, col
import re
import os
#UPLOAD FILES FROM LOCAL COMPUTER TO GOOGLE CLOUD STORAGE

# START get_all_csv

def get_all_csv(file_dir, pattern = r'^part', host = 'databricks'):
  """
  Return a list of all csv files saved from train and test set
  pattern default = r'^part" - all csv files that start with the word 'part'
  """
  
  # Store the path and name of all the csv file
  list_csv = []
  
  # List all the files in file_dir

  if (host == 'local'):
    all_files = os.listdir(file_dir)

  else:
    all_files = os.listdir('/dbfs' + file_dir)
  
  part_regex = re.compile(pattern)    
  
    
  for ele in all_files:
    # Store the path and name of the csv file matching pattern
    #csv_file = [] 
    part_search = part_regex.search(str(ele))

    # If we match a file with pattern
    if (part_search != None):
       list_csv.append(ele)
           
 
  return list_csv

#END get_all_csv

# START upload_to_gcs
def upload_to_gcs(file_dir, src_name, dest_name, bucket, host):
  
  if (host == 'local'):
    os.chdir(file_dir)
  else:
    os.chdir("/dbfs" + file_dir)

  blob = bucket.blob(dest_name)
  blob.upload_from_filename(src_name)
   
# END upload_to_gcs


# START create_bucket
def create_bucket(bucket_name, storage_location = 'us-central1', host = 'databricks'):
  """
  Return : bucket
  Create bucket named bucket_name 
  Upload file name src_name from file_dir to GSC and name it dest_name location 'us-central1' as default
  """
  from google.cloud import storage
  storage_client = storage.Client()

  # Name a bucket - has to be unique because it has to be globally recognizable 
  bucket = storage.Bucket(storage_client, bucket_name)
  
  #add location of bucket
  bucket.create(location= storage_location)
  
  return bucket
# END create_bucket



# COMMAND ----------


# START import_data_to_automl
def import_data_to_automl(bucket_name, automlds_display_name, ds_name_in_gsc, storage_location = 'us-central1', project_id = "myprojectcas02", host = 'databricks'):
  """

  """

  from google.cloud import automl_v1beta1 as automl

  #Initiate Automl client api object
  automl_client = automl.AutoMlClient()
  #automl_prediction_client = automl.PredictionServiceClient()
   
  # parent == project location 

  parent = automl_client.location_path(project= project_id, location= storage_location)


  # Define AutoML Dataset info

  # Metadata of dataset
  dataset_data_dict = {
      "display_name": automlds_display_name,
      "tables_dataset_metadata": {}
  }

  dataset = automl_client.create_dataset(parent= parent, dataset= dataset_data_dict)

  # Get dataset_id 

  dataset_id = dataset.name.split('/')[-1]

  # Set dataset full id(/path)
  dataset_full_id = automl_client.dataset_path(project= project_id, location= storage_location, dataset= dataset_id)

  # Create gs path to dataset

  path_to_dataset = "gs://" + bucket_name + "/" +  ds_name_in_gsc

  # Import data from GCS to AutoML Table

  input_uris = path_to_dataset.split(',')

  input_config = {"gcs_source": {"input_uris": input_uris}}

  response = automl_client.import_data(dataset_full_id, input_config)

  from datetime import datetime

  start_time = datetime.now()
  response.result()
  print ("Successfully imported data into AutoML. It took ", datetime.now() - start_time, " to import data from GSC to AutoML.")

  
# END import_data_to_automl

# COMMAND ----------

#START upload_data
def upload_data(file_dir, ds_name, try_name, train_or_test = 'train', project_id = "myprojectcas02",storage_location = 'us-central1', host = 'databricks'):
  
  bucket_name = ds_name  + "_" + train_or_test + "_" + str(try_name)
  
  automlds_display_name = bucket_name
  
  gcs_dest_name = ""
  
  #Step 1: Create bucket on GCS
  bucket = create_bucket(bucket_name, storage_location, host)
  
  #Step 2: Gather all csv files and upload to GCS
  all_csvs = get_all_csv(file_dir, r'^part', host)
  for csv in all_csvs:
    num = 0
    if (train_or_test == 'train'):
      dest_name = ds_name + "_train" + "_" + try_name + 'th' + str(num) + '.csv'
    else:
      dest_name = ds_name + "_test" + "_" + try_name + 'th' + str(num) + '.csv'
    upload_to_gcs(file_dir, csv, dest_name, bucket, host)
    num += 1
    import_data_to_automl(bucket_name, automlds_display_name, dest_name, storage_location, project_id, host)
 
  
  
#END upload_data
# COMMAND ----------




