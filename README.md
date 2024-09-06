<h3 align="center">Data Analysis with SQL and Python</h3>

##  About
This project was created to analyze the data from the UCI Machine Learning Repository's Online Retail Dataset. First, a database and schema were created in a Postgres Database. Then, a python connection with the data was established, generating a pandas dataframe. The data types were adjusted and the missing values replaced by 0 in the CustomerID column. Finally, a csv was generated from the pandas dataframes and copied into a table inside the Postgres database. A few queries were executed to analyze the dataset and some visualizations generated. 

## Getting Started
Before executing the code, make sure that the necessary packages are installed, all files are in the same folder and the terminal is being executed in the same folder.

## Project Requirements
The following instructions were received:
- Ingest the dataset into a SQL database.
- Write complex SQL queries to: 
  - Find the top 10 customers by total purchase amount. 
  - Identify the most popular products based on the number of orders. 
  - Calculate the monthly revenue for the dataset's time range. 
- Use Python to: 
  - Connect to the SQL database and retrieve the results of the above queries. 
  - Generate visualizations (e.g., bar charts, line graphs) for the analysis performed. 
  - Save the visualizations as image files.

## SQL Script
The SQL script created contain only the queries to be executed in the analysis part. 
The creation of the Database cannot be executed with the python script, so it was executed in the Postgres interface pgAdmin 4:
```
CREATE DATABASE data_engineer
```
Schema and Table creation were generate inside the python file.

## Python Script
The Python script is organized as follows:
- main() function to orchestrate the entire process, containing all variables needed and the steps to be executed.
- Every step has its own function, with the corresponding docString explaining the purpose of it, and the parameters needed

## Database INI Script
File containing all information needed to connect to the Postgres database. This file MUST be in the .gitignore file for real projects, since it contains the information to access the database.

<h3 align="center">Sales Data Transformation and Aggregation </h3>

##  About
This project was created to analyze the data from a large sales dataset. First, a .csv file is read using spark dataframe. The data types were adjusted and the missing values replaced by 0 in the CustomerID column. With the data cleaned and the right data types, a few analysis were made using PySpark language and printed in the screen (just for the purpose of the exercise, real pipelines should avoid unnecessary actions in a spark pipeline). Finally, the dataset is written into a Postgres Database. 

## Getting Started
Before executing the code, make sure that the necessary packages are installed, all files are in the same folder and the terminal is being executed in the same folder.
It is necessary to download the .csv file from the following link: https://www.kaggle.com/datasets/carrie1/ecommerce-data
The file should be saved in the same project directory and named 'data.csv'

## Project Requirements
The following instructions were received:
- Load the dataset into a PySpark DataFrame. 
- Perform necessary data cleaning (e.g., handle missing values, correct data types). 
- Calculate total sales and the number of transactions per day. 
- Identify the top 10 products with the highest sales. 
- Save the transformed data back to a SQL database. 

## Python Script
The Python script is organized as follows:
- main() function to orchestrate the entire process, containing all variables needed and the steps to be executed.
- Every step has its own function, with the corresponding docString explaining the purpose of it, and the parameters needed

## Database INI Script
File containing all information needed to connect to the Postgres database. This file MUST be in the .gitignore file for real projects, since it contains the information to access the database.

