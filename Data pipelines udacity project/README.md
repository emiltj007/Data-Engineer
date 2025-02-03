# Data Pipelines with Airflow for a music streaming company, SPARKIFY



# Project details

Sparkify, has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines and come to the conclusion that the best tool to achieve this is Apache Airflow.

* create high grade data pipelines that are dynamic and built from reusable tasks, can be monitored, and allow easy backfills;
* data quality plays a big part when analyses are executed on top the data warehouse and want to run tests against their datasets after the ETL steps have been executed to catch any discrepancies in the datasets;

The source data resides in S3 and needs to be processed in Sparkify's data warehouse in Amazon Redshift. The source datasets consist of JSON logs that tell about user activity in the application and JSON metadata about the songs the users listen to.



# Project Environment

### AWS Environment

* Amazon redshift serverless
* Amazon redshift clusters
* AWS S3
* IAM

### Pipeline
* Apache Airflow

### Workspace
* Visual studio code


# Project Data
Datasets
For this project, working with two datasets. Here are the s3 links for each:

Log data: s3://udacity-dend/log_data
Song data: s3://udacity-dend/song-data

Copy S3 Data
The data for the project is stored in Udacity's S3 bucket. This bucket is in the US West AWS Region. To simplify things, we will copy the data to new bucket in the same AWS Region where you created the Redshift workgroup so that Redshift can access the bucket.


# Project Template

* The dag template has all the imports and task templates in place, but the task dependencies have not been set
* The operators folder with operator templates
* A helper class for the SQL transformations


#  Graph

![Graph.jpg](attachment:Graph.jpg)



# Highlights

## Files
1. /home/workspace/airflow/plugins/final_project_operators/data_quality.py
2. /home/workspace/airflow/plugins/final_project_operators/load_fact.py
3. /home/workspace/airflow/plugins/final_project_operators/load_dimensions.py
4. /home/workspace/airflow/plugins/final_project_operators/stage_redshift.py
5. /home/workspace/airflow/dags/udacity/common/final_project_sql_statements.py
6. /home/workspace/airflow/dags/cd0031-automate-data-pipelines/project/starter/final_project.py


# Steps Perfromed


1. Created S3 directories for log-data, song-data, and json metadata, and copied the data from udacity s3 location as a starting point.
2. Below 4 operators were built to stage the data, load fact table, load dimensional table and perfor data quality check.

   ### *  stage_redshift.py
           a. load any JSON-formatted files from S3 to Amazon Redshift
           b. operator creates and runs a SQL COPY statement based on the parameters provided
           
   ### *  load_fact.py
           a. utilize the provided SQL helper class (final_project_sql_statements.py) to run data transformations
           b. Most of the logic is within the SQL transformations, and the operator is expected to take as input a SQL statement and target database on which to run the query against
           c. allow append type functionality
           
   ### *  load_dimensions.py
           a. utilize the provided SQL helper class (final_project_sql_statements.py) to run data transformations
           b. Most of the logic is within the SQL transformations, and the operator is expected to take as input a SQL statement and target database on which to run the query against
           c. truncate-insert pattern
           
   ### *  data_quality.py
          a. runs checks on the data itself. The operator's main functionality is to receive one or more SQL based test cases along with the expected results and execute the tests. For each test, the test result and expected result need to be checked, and if there is no match, the operator should raise an exception, and the task should retry and fail eventually.
          
3. DAG is 
### final_project.py
    * Make use of the operators and is run in Apache airflow to stage the table, load fact and dimentional tables and perfom data quality checks.
    * The tabbles (staging, fact and dimensional) are created in the between the process using helper class final_project_sql_statements.py.
   






```python

```
