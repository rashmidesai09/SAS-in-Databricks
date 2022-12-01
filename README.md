# SAS-in-Databricks

<img width="600" alt="image" src="https://user-images.githubusercontent.com/97893144/205053242-45d77733-fd95-44d3-bb5b-7d1b02b80d2f.png">

## Reading SAS data files into Databricks

### SAS dataset libraries and Databricks databases and tables
In SAS, a library points to a particular location on a drive where a collection of sas datasets are stored. This could be on a local network drive, a filesystem, or a remote database. Using the libname assigned to a library allows you to reference a dataset within a library.

In Databricks, this is equivalent to creating a metastore database that points to some unmanaged tables and allows you to persist the tables outside of a session or an individual cluster.

A Databricks database is a collection of tables. A Databricks table is a collection of structured data. Filter, and perform any operations supported by Apache Spark DataFrames on Databricks tables. Also query tables with Spark APIs and Spark SQL.

There are two types of tables: global and local.
- A global table is available across all clusters. Databricks registers global tables either to the Databricks Hive metastore or to an external Hive metastore.
- A local table is not accessible from other clusters and is not registered in the Hive metastore. This is also known as a temporary view.

### Data storage and access
Using SAS, the filename is used to read data from external sources. In Databricks, the Databricks File System (DBFS) is used to access data in object storage.
Databricks File System (DBFS) is a distributed file system mounted into a Databricks workspace and available on Databricks clusters. DBFS is an abstraction on top of scalable object storage and offers the following benefits:

### Read a dataset from a specified location
In SAS, reading a dataset is done using the set method:
Data a; Set b; Run;

There are several methods we can use to read a SAS dataset in and write to a Databricks format:
- Write to a Spark DataFrame using Python
- Write to a SQL table or a temporary view using Python or SQL
- Write to a Delta table using Python

As part of this project, we will load a sas7bdat file using the saurfang library format option. In order to read in sas7bdat files, we need to use the saurfang library Below are the steps to installed in our cluster -

<img width="600" alt="image" src="https://user-images.githubusercontent.com/97893144/205107511-ad30c62f-b40c-4e95-a194-c87729c15b7c.png">

Libraries --> Install new --> Maven

<img width="600" alt="image" src="https://user-images.githubusercontent.com/97893144/205107656-bd0e1dd4-e254-4911-8eda-f35d768b9855.png">

Select Maven Central

<img width="600" alt="image" src="https://user-images.githubusercontent.com/97893144/205107879-1c24fada-2b75-4866-842d-d546faa238c5.png">

Search and Install as below -

<img width="600" alt="image" src="https://user-images.githubusercontent.com/97893144/205108389-d50182ca-4e1c-4a29-8ffb-901ee1c706bd.png">

Also Install -

<img width="600" alt="image" src="https://user-images.githubusercontent.com/97893144/205108725-4fc94656-0e43-47a6-b76a-45b540fdc784.png">

Restart the cluster after successful installation.

Below is the method used for loading the dataset:
1. Loading the data and writing it to a Spark DataFrame using Python.

The code block below returns a DataFrame df. A DataFrame is a two-dimensional labeled data structure with columns of potentially different types. You can think of a DataFrame like a spreadsheet, a SQL table, or a dictionary of series objects.

The Spark DataFrameReader is called via spark.read and is similar to the proc import command in SAS. The DataFrameReader can take in many file formats, such as json, parquet, csv, and many more.

<img width="600" alt="image" src="https://user-images.githubusercontent.com/97893144/205112202-217597cd-44f4-4c7b-a0f8-375f5670ee5c.png">


NOTE: Here, we specify the input schema for the data by listing the field names and their data types. If we do not specify a schema, the DataFrameReader will infer one by sampling some of the input file.

### Write to a table or a temporary view using PySpark
Before you can issue SQL queries on a DataFrame, you must save it as a table or temporary view. Temporary views are removed once your session has ended, whereas tables are persisted beyond a given session.

<img width="600" alt="image" src="https://user-images.githubusercontent.com/97893144/205115209-3ee760b5-1b80-48ac-a991-ac80f2238b0e.png">


### Write to a Delta table using Python

<img width="600" alt="image" src="https://user-images.githubusercontent.com/97893144/205115344-94ab3a0f-c8cc-41b4-92bf-54302b273356.png">

<img width="973" alt="image" src="https://user-images.githubusercontent.com/97893144/205116001-e0dc34af-ca9f-41c8-a9c9-57d39f5daa48.png">

## Performing SAS DATA Step operations in Databricks for a global sales data
### SAS DATA steps are generally used to:

- Read input files
- Work with variables and numbers
- Merge datasets (unions and joins)

### Setting up the Data

<img width="600" alt="image" src="https://user-images.githubusercontent.com/97893144/205117034-cfec2854-16af-421f-abd0-02612a32aa32.png">

<img width="600" alt="image" src="https://user-images.githubusercontent.com/97893144/205117123-daac5870-e576-4570-ac1b-f08a2119b3dc.png">

View the schema of a DataFrame to see the data types of each variable:

<img width="600" alt="image" src="https://user-images.githubusercontent.com/97893144/205117417-2b78d325-19e8-433d-b69e-3d4dd452d863.png">

### Performing DATA Steps
1. Merge 
We will merge retailers_df and transactions_df on the retailer_id field, first using PySpark, then using Spark SQL, using the join function.

NOTE: With Spark, you do NOT have to sort datasets before performing operations on them!

<img width="600" alt="image" src="https://user-images.githubusercontent.com/97893144/205118076-3c924fcf-028b-45cf-8d67-b6632b8b7f3a.png">

2. Working with Dates
DATE
To return the current date like you would do with the SAS today() function, use the function current_date.

TODAY
If you have a date in the number of seconds from unix epoch (1970-01-01 00:00:00 UTC) format, you can convert it to a string representing the datetime in a specified timezone using the from_unixtime function.

YEAR
To extract the year from a date as an integer, like with the SAS year() function, use the year function.

In pyspark it can be done as follows -

<img width="600" alt="image" src="https://user-images.githubusercontent.com/97893144/205118617-1106ed0d-f08c-4a59-b597-86df061c3de1.png">

Extract various date formats from a datetime column using date_format and create new columns using withColumn:

<img width="600" alt="image" src="https://user-images.githubusercontent.com/97893144/205118816-dddb4279-645f-4710-b739-1128d5d8e1e0.png">

3. Working with strings
To extract a substring from a string, as in the SAS function substr(), use the substring function.
The trim function in PySpark to perform the same functions as strip() in SAS.

<img width="600" alt="image" src="https://user-images.githubusercontent.com/97893144/205119091-63ceb336-e537-4ecb-b693-5aad428d0766.png">

Same action as the SAS lowcase() function using LOWER() in SQL or PySpark:

<img width="600" alt="image" src="https://user-images.githubusercontent.com/97893144/205119616-43ab3d16-5845-4d73-a4a7-c8d34c27533e.png">

4. Working with columns
In SAS, you use IF/THEN/ELSE and WHERE to create columns based on business logic.

In Pyspark it can be done as -

<img width="600" alt="image" src="https://user-images.githubusercontent.com/97893144/205119973-7d7b85d5-8aec-4c63-ac26-f308cf3fa298.png">

5. Working with Data types
INPUT and PUT convert a character value into a numeric value or vice versa, in SAS. The PySpark cast function achieves the same.

<img width="600" alt="image" src="https://user-images.githubusercontent.com/97893144/205120322-c0ee0efa-ca98-4614-ac77-9665ed351bd5.png">

6. Working with Numeric Data types
All of the mathematical functions  in SAS, such as: CEIL , INT , ROUND ,MEAN, MIN, MAX can be performed using PySpark or SQL functions.

<img width="600" alt="image" src="https://user-images.githubusercontent.com/97893144/205120888-85a412ce-5d65-4edf-8196-bf70e7ea65dc.png">

<img width="600" alt="image" src="https://user-images.githubusercontent.com/97893144/205120949-5521d59f-b1a3-4797-80eb-50ed96900782.png">

<img width="600" alt="image" src="https://user-images.githubusercontent.com/97893144/205121163-27522e7a-48fd-4af1-8c1b-0b63fb7ba757.png">

## Performing SAS PROC Step operations in Databricks

1. PROC SQL is generally used to:
- generate reports and summary statistics
- retrieve or combine data from tables or views
- create tables, views, and indexes
- update the data values in PROC SQL tables
- update and retrieve data from database management system (DBMS) tables
- modify a PROC SQL table by adding, modifying, or dropping columns


In databricks, perform any PROC SQL operations on Databricks using Spark SQL almost exactly the same as in SAS.
Simply use %sql or spark.sql("some code") with the SQL code you want to use.

Replace many PROC SQL steps with Pyspark.sql.functions, such as:
filter()
join()
withColumn()
concat()
agg(sum())
orderBy()

2. PROC SORT
Do NOT have to sort datasets before performing operations in Spark. However, there may be times the data needs to sort for visualization purposes. To do this, use the orderBy method. Sort is possible on multiple fields and by ascending or descending order, per field.

Chain multiple operations into one statement as below -

<img width="600" alt="image" src="https://user-images.githubusercontent.com/97893144/205122408-c9948e11-d211-4475-be65-e383247faa01.png">


3. Common PROC Steps  for basic Statistical operations

Basic statistical summary on a DataFrame (numerical data only) using the .summary() function.

<img width="600" alt="image" src="https://user-images.githubusercontent.com/97893144/205122624-5b1b1793-ed84-4090-b191-c645b326a441.png">


