# Real Estate ETL Pipeline

## Table of Contents
1. [Project Overview](#project-overview)
2. [Project Architecture](#project-architecture)
3. [Data Flow](#data-flow)
4. [Prerequisites](#prerequisites)
5. [Technologies Used](#technologies-used)
6. [Input Files](#input-files)
7. [Output Files](#output-files)
8. [Conclusion](#conclusion)

## Project Overview
The Real Estate ETL Pipeline is an end-to-end data processing pipeline that extracts, transforms, and loads real estate data using PySpark and Apache Airflow. This pipeline processes property and owner data stored in Google Cloud Storage (GCS), performs transformations to calculate additional metrics, and loads the processed data into BigQuery and a GCS bucket for further analysis.

## Project Architecture
![Project Architecture](https://github.com/malviya1908/airflow-pyspark-real-estate-pipeline/blob/main/architecture/real_estate_architecture.png)
The project utilizes a combination of Google Cloud and open-source technologies:
- **Google Cloud Dataproc**: For executing PySpark jobs.
- **Google Cloud Storage (GCS)**: For storing input and output files.
- **Google BigQuery**: For storing processed data for analysis.
- **Google Cloud Logging**: For monitoring and troubleshooting the pipeline workflow.
- **Apache Airflow**: For orchestrating the workflow.

## Data Flow
1. **Input**: Raw JSON files for real estate and owner data stored in GCS.
2. **Transformation**: Data transformations in PySpark:
   - Calculation of `Price_Per_Sqft`.
   - Renaming columns for consistency.
   - Joining real estate and owner datasets.
3. **Output**:
   - Processed data loaded into BigQuery.
   - CSV files saved in a GCS bucket.
  
## Prerequisites
To run this project, ensure the following prerequisites are met:
- Google Cloud Platform (GCP) account.
- Configured Google Cloud SDK on your local system.
- Airflow installed and configured with the required providers.
- Proper IAM roles and permissions in GCP for accessing GCS, BigQuery, Dataproc, Composer(Apache Airflow), and Logging APIs.

## Technologies Used
- **Google Cloud Platform (GCP)**:
  - Dataproc
  - Cloud Storage
  - BigQuery
  - Cloud Logging
- **Apache Airflow**:
  - For workflow orchestration.
- **PySpark**:
  - For data transformations.
- **Python**:
  - For scripting and program logic.
 
## Input Files
The pipeline processes the following input files:
1. **Real Estate Data**:
   - Contains details such as `Property_ID`, `Location`, `Type`, `Size_SqFt`, `Price`, `Bedrooms`, `Year_Built`, and `Bathrooms`.

2. **Owner Data**:
   - Contains details such as `Owner_ID`, `Property_ID`, `Owner_Name`, and `Contact_Number`.
  


## Output Files
The pipeline generates the following outputs:
1. **BigQuery Table**:
   - Dataset: `Real_estate_dataset`
   - Table: `real_estate_owner_data`
   - Contains combined real estate and owner data.

2. **GCS Output**:
   - Format: CSV
   - Contains processed data with additional fields like `Price_Per_Sqft`.

3. **Logs**:
   - Accessible via **Google Cloud Logging**.
   - Logs include cluster creation, PySpark job execution, and cluster deletion details for monitoring and debugging.
  
## Conclusion
This Real Estate ETL Pipeline demonstrates a scalable and automated approach to data processing using PySpark, Airflow, and GCP services. The integration with Google Cloud Logging enhances monitoring and debugging, ensuring better operational visibility and error handling.

