from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import google.cloud.logging
import logging
import datetime

# Initializing Google Cloud Logging
logging_client = google.cloud.logging.Client()
logging_client.setup_logging()
logger = logging.getLogger('realEstate-processing-pipeline')

# Function for setting up the google cloud logging
def log_pipeline_step(step, message, level="INFO"):
    if level == "INFO":
        logger.info(f"Step: {step}, Message: {message}")
    elif level == "ERROR":
        logger.error(f"Step: {step}, Error: {message}")
    elif level == "WARNING":
        logger.warning(f"Step: {step}, Warning: {message}")

# Function for Creating a spark session
def create_sparkSession(app):
    return SparkSession.builder.appName(app).getOrCreate()

# Function for reading the files in GCS bucket into PySpark DataFrame
def read_file_into_dataframe(spark, fileType, gcs_source_bucket, inferschema=True, mode="PERMISSIVE"):
    return spark.read.format(fileType)\
                .option("inferschema", inferschema)\
                .option("mode", mode)\
                .load(gcs_source_bucket)

# Function for making the transformations in the DataFrames
def pyspark_transformation(real_estate_df, owner_df):
    # Calculating the Price_Per_Sqft in real_estate_df
    real_estate_with_price_per_sqft = real_estate_df.withColumn("Price_Per_Sqft", round(col("Price") / col("Size_SqFt"), 2))
    log_pipeline_step("Transformation - 1", "Successfully calculated the Price_Per_Sqft")

    # Renaming columns for Property_ID in both the DataFrames
    real_estate_renamed = real_estate_with_price_per_sqft.withColumnRenamed("Property_ID", "Real_Estate_Property_ID")
    owner_renamed = owner_df.withColumnRenamed("Property_ID", "Owner_Property_ID")
    log_pipeline_step("Transformation - 2", "Successfully renamed both the DataFrames")

    return real_estate_renamed, owner_renamed

# Function to join the transformed DataFrames
def join_dfs(transformed_real_estate_df, transformed_owner_df):
    joined_df = transformed_real_estate_df\
        .join(transformed_owner_df, transformed_real_estate_df["Real_Estate_Property_ID"] == transformed_owner_df["Owner_Property_ID"], "inner")\
        .selectExpr("Owner_Property_ID", "Owner_ID", "Owner_Name", "Contact_Number", "Location", "Type", "Size_SqFt", "Price", "Price_Per_Sqft", "Bathrooms", "Bedrooms")
    return joined_df

# Main processing function
def process_data():
    # Setting up variables
    today_date = datetime.datetime.now().strftime("%Y-%m-%d")

    # File locations in GCS bucket
    real_estate_data = f"gs://project-bucket-for-pipeline/real_estate_data/real_estate_files/{today_date}/real_estate_data.json"
    real_estate_file = real_estate_data.split("/")[-1]

    owner_data = f"gs://project-bucket-for-pipeline/real_estate_data/owners_data_files/{today_date}/owner_data.json"
    owner_file = owner_data.split("/")[-1]

    # BigQuery details
    dataset_id = "linen-age-447106-e3.Real_estate_dataset"
    table_name = "real_estate_owner_data"
    table_id = f"{dataset_id}.{table_name}"
    bq_temp_bucket = "gs://project-bucket-for-pipeline/real_estate_data/gcs_temp_bucket"

    # Backup location in GCS bucket
    final_df_bucket = f"gs://project-bucket-for-pipeline/real_estate_data/joined_real_estate_owner/{today_date}"

    try:
        # Creating a Spark session
        log_pipeline_step("Spark Session", "Creating a Spark session")
        spark = create_sparkSession("Real_Estate_Data_Processing")
        log_pipeline_step("Spark Session", "Successfully created the Spark session")

        # Reading files into DataFrames
        log_pipeline_step("Read File 1", f"Reading the {real_estate_file} file")
        real_estate_df = read_file_into_dataframe(spark, "json", real_estate_data)
        log_pipeline_step("Read File 1", f"Successfully read the {real_estate_file} file")

        log_pipeline_step("Read File 2", f"Reading the {owner_file} file")
        owner_df = read_file_into_dataframe(spark, "json", owner_data)
        log_pipeline_step("Read File 2", f"Successfully read the {owner_file} file")

        # Applying transformations
        log_pipeline_step("Transformation", "Starting transformations")
        transformed_real_estate_df, transformed_owner_df = pyspark_transformation(real_estate_df, owner_df)

        # Joining DataFrames
        log_pipeline_step("Transformation - 3", "Joining the DataFrames")
        joined_df = join_dfs(transformed_real_estate_df, transformed_owner_df)
        log_pipeline_step("Transformation - 3", "Successfully joined the DataFrames")

        # Writing to BigQuery
        log_pipeline_step("Data Write - 1", f"Writing data to BigQuery table: {table_name}")
        joined_df.write\
            .format("bigquery")\
            .option("table", table_id)\
            .option("temporaryGcsBucket", bq_temp_bucket)\
            .mode("append")\
            .save()
        log_pipeline_step("Data Write - 1", f"Successfully wrote data to BigQuery table: {table_name}")

        # Writing to GCS bucket
        log_pipeline_step("Data Write - 2", "Writing data to GCS bucket")
        joined_df.write\
            .format("csv")\
            .option("header", True)\
            .mode("append")\
            .save(final_df_bucket)
        log_pipeline_step("Data Write - 2", "Successfully wrote data to GCS bucket")

    except Exception as e:
        log_pipeline_step("Exception", f"Error: {str(e)}", level="WARNING")
        print(f"Error: {str(e)}")

    finally:
        # Stopping the Spark session
        log_pipeline_step("Stop Spark Session", "Stopping the Spark session")
        if spark:
            spark.stop()
        log_pipeline_step("Stop Spark Session", "Successfully stopped the Spark session")

# Execute the processing function
if __name__ == "__main__":
    log_pipeline_step("Pipeline Started", "Real Estate processing pipeline initiated.")
    process_data()
    log_pipeline_step("Pipeline Ended", "Real Estate processing pipeline completed.")