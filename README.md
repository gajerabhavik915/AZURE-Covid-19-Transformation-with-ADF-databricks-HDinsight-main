----------------------------------------------------------------
# AZURE-Covid-19-Transformation-with-ADF-databricks-HDinsight
----------------------------------------------------------------
in this project, we use Microsoft Azure cloud services to get Extract, Transformation, and Load (ETL) done on the COVID-19 dataset. we are going to create
Production level Azure data factory pipeline that depended on another parent pipeline and also make triggers for executing those pipelines for Transformations.
we store that transformed data into Azure SQL database. in short, we are making the data platform, where Data Analysts and Data scientist can start their works 
and find some useful insight in order to make decisions.


-------------------
# Objective : 
-------------------
Create a data platform for our data scientist and data analyst team where they can run a machine learning model to 
predict the spread of CORONA VIRUS.

--------------------------
# Used Technologies : 
--------------------------

1) Azure DataFactory
2) Azure HDinsight (Hive)
3) Azure Databricks (Pyspark, SparkSql)
4) Azure Storage Account
5) Azure Data lake Gen2
6) Azure SQL Database

---------------------
# Solution
---------------------

STEP-1: Create required Resources e.g. storage containers and other...
------------------------------------------------------------------------------------------------------------
STEP-2: Create a pipeline for ingesting data into the data lake gen2 container from HTTP and BLOB.
------------------------------------------------------------------------------------------------------------
STEP-3: Transform "cases_and_deaths.csv" and "hospital_admissions.csv" using Data-flow activities. 
------------------------------------------------------------------------------------------------------------
STEP-4: Mount required containers ('raw', 'lookups', 'processed') on Databricks via "mount.py". 
------------------------------------------------------------------------------------------------------------
STEP-5: Transform "population.csv" file via Azure DataFactory Notebook activity with Standard Databricks 
        cluster.
------------------------------------------------------------------------------------------------------------
STEP-6: Provision Hdinsight cluster and execute via Azure DataFactory Hdinsight-hive activity for the run 
        Hive script which creates external tables and finally writes the transformed and aggregated data.
------------------------------------------------------------------------------------------------------------        
STEP-7: Copy transformed data from data lake container to Azure SQL Database via ADF copy activity.
------------------------------------------------------------------------------------------------------------
STEP-8: Make production-ready our pipelines by Orcharstrate All pipelines.
------------------------------------------------------------------------------------------------------------
STEP-9: Schedule and monitor our pipelines with Azure Datafactory.
------------------------------------------------------------------------------------------------------------


