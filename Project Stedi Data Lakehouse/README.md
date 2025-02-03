
# STEDI Human Balance Analytics project



# Project details

STEDI Step Trainer performs the following:

* trains the user to do a STEDI balance exercise;
* and has sensors on the device that collect data to train a machine-learning algorithm to detect steps;
* has a companion mobile app that collects customer data and interacts with the device sensors.

STEDI has heard from millions of early adopters who are willing to purchase the STEDI Step Trainers and use them.

Several customers have already received their Step Trainers, installed the mobile application, and begun using them together to test their balance. The Step Trainer is just a motion sensor that records the distance of the object detected. The app uses a mobile phone accelerometer to detect motion in the X, Y, and Z directions.

The STEDI team wants to use the motion sensor data to train a machine learning model to detect steps accurately in real-time. Privacy will be a primary consideration in deciding what data can be used.

STEDI team has built a data lakehouse solution for sensor data that trains a machine learning model.
As the data engineer for the STEDI team, I have extracted the data produced by the STEDI Step Trainer sensors and the mobile app, and curated them into a data lakehouse solution on AWS so that Data Scientists can train the learning model.


# Project Environment

### AWS Environment
Date is extracted and curated for the machine learning model using:

* Python and Spark
* AWS Glue
* AWS Athena
* AWS S3

### Github Environment
Github repository is used to store the SQL scripts and Python code in. 

### Workflow Environment Configuration
Python scripts created using AWS Glue and Glue Studio. These web-based tools and services contain multiple options for editors to write or generate Python code that uses PySpark. 


# Project Data
STEDI has three JSON data sources in the Github repo:

customer
step_trainer
accelerometer

Here are the steps to download the data:

Go to nd027-Data-Engineering-Data-Lakes-AWS-Exercises(opens in a new tab) repository and click on Download Zip.
Download repo as a Zip file.
Download repo as a Zip file.

Extract the zip file.

Navigate to the project/starter folder in the extracted output to find the JSON data files within three sub-folders. You should have

956 rows in the customer_landing table,
81273 rows in the accelerometer_landing table, and
28680 rows in the step_trainer_landing table.
### 1. Customer Records
This is the data from fulfillment and the STEDI website.

Data Download URL(opens in a new tab)

AWS S3 Bucket URI - s3://cd0030bucket/customers/

contains the following fields:

* serialnumber
* sharewithpublicasofdate
* birthday
* registrationdate
* sharewithresearchasofdate
* customername
* email
* lastupdatedate
* phone
* sharewithfriendsasofdate

### 2. Step Trainer Records
This is the data from the motion sensor.

Data Download URL(opens in a new tab)

AWS S3 Bucket URI - s3://cd0030bucket/step_trainer/

contains the following fields:

* sensorReadingTime
* serialNumber
* distanceFromObject

### 3. Accelerometer Records
This is the data from the mobile app.

Data Download URL(opens in a new tab)

AWS S3 Bucket URI - s3://cd0030bucket/accelerometer/

contains the following fields:

* timeStamp
* user
* x
* y
* z



# Data flow

![flowchart.jpg](attachment:flowchart.jpg)



# Highlights
## Counts
* Total count of distinct records customer_landing: 956
* Total count of distinct records accelerometer_landing: 81273
* Total count of distinct step_trainer_landing: 28680

* Total count of customer_trusted: 482
* Total count of accelerometer_trusted: 40981
* Total count of distinct step_trainer_trusted: 14460

* Total count of distinct customer_curated: 482
* Total count of distinct machine_learning_curated: 40981

## sql files
* customer_landing.sql
* accelerometer_landing.sql
* step_trainer_landing.sql

* customer_trusted.sql
* accelerometer_trusted.sql
* step_trainer_trusted.sql

* customer_curated.sql


##  Jobs

 * customer_landing_to_truster.py
 * accelerometer_landing_to_trusted.py
 * customer_trusted_to_curated.py
 * machine_learning_curated.py
 
## Screenshots

* customer_landing.jpg
* accelerometer_landing.jpg
* step_trainer_landing.jpg

* customer_trusted.jpg
* accelerometer_trusted.jpg
* step_trainer_trusted.jpg

* customer_curated.jpg
* machine_learnining_curated.jpg




# Steps Perfromed


1. Created S3 directories for customer_landing, step_trainer_landing, and accelerometer_landing zones, and copied the data there as a starting point.
2. Created two Glue tables for the two landing zones with raw data: 
    #### * customer_landing
            a. Created table in S3 bucket using customer_landing.sql
            b. Total count of distinct records : 956
    #### * accelerometer_landing       
            a. Created table in S3 bucket using accelerometer_landing.sql
            b. Total count of distinct records : 81273
3. Using Glue studio, 2 AWS glues jobs created to:
    #### * customer_landing_to_truster.py
            a. Created table customer_trusted using customer_trusted.sql
            b. Using glue job, sanitized customer landing data was loaded to customer_trusted by removing duplicates and fetched only customer records who agreed top share their data for research purposes. Field shareWithResearchAsOfDate was used to perform the filteration.
            c. Total count of customer_trusted: 482
    #### * accelerometer_landing_to_trusted.py
            a. Created table accelerometer_trusted using accelerometer_trusted.sql
            b. Using glue job, sanitized accelerometer landing data was loaded to accelerometer_trusted by only fetching Accelerometer Readings from customers who agreed to share their data for research purposes (customer_trusted).
            c. Total count of accelerometer_trusted: 40981
4. Due to a data quality issue with the Customer Data further steps were performed. The serial number should be a unique identifier for the STEDI Step Trainer they purchased. However, there was a defect in the fulfillment website, and it used the same 30 serial numbers over and over again for millions of customers! Most customers have not received their Step Trainers yet, but those who have, are submitting Step Trainer data over the IoT network (Landing Zone). The data from the Step Trainer Records has the correct serial numbers. 
    
    a. customer_curated table was created in S3 bucket using customer_curated.sql
    
    b. Using glue job customer_trusted_to_curated.py, sanitized customer trusted data was loaded to customer_curated table by only including customers who have accelerometer data and have agreed to share their data for research.
        * Total count of distinct customer_curated: 482
    
    c. step_trainer_landing table was created with step_tainer raw data using step_trainer_landing.sql
        * Total count of distinct step_trainer_landing: 28680
    
    d. step_trainer_trused table was created using step_trainer_trusted.sql
    
    e. Using glue job, step_trainer_trusted was loaded with Step Trainer Records data for customers who have accelerometer data and have agreed to share their data for research (customers_curated).
         * Total count of distinct step_trainer_trusted: 14460
    f. Using glue job machine_learning_curated.py, machine_learning_curated table was created and loaded with aggregated data that has each of the Step Trainer Readings, and the associated accelerometer reading data for the same timestamp, but only for customers who have agreed to share their data.
         * Total count of distinct machine_learning_curated: 40981

# Issues

Glue job threw a lot of issues and was very unpredictable. Data catalog tables created from glue job seems had issues loading data while using those tables as source in other glue jobs. The workaround was to create glue table separately using data catalog  and then use the table as target in the job to load the required data. With this method, these tables worked correctly while using as source for other glue jobs.

The last glue table machine_learner_curated was created through glue job and not separately through data catalog which I hope satisfies the criteria in the project rubric.



```python

```
