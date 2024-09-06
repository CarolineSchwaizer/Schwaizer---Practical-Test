<h3 align="center">Data Analysis with SQL and Python</h3>

##  About
This project was created to analyze the data from the UCI Machine Learning Repository's Online Retail Dataset. First, a database and schema were created in a Postgres Database. Then, a python connection with the data was established, generating a pandas dataframe. The data types were adjusted and the missing values replaced by 0 in the CustomerID column. Finally, a csv was generated from the pandas dataframes and copied into a table inside the Postgres database. A few queries were executed to analyze the dataset and some visualizations generated. 

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
 
## Getting Started
Before executing the code, make sure that the necessary packages are installed, all files are in the same folder and the terminal is being executed in the same folder.

## SQL Script
The SQL script created contain only the queries to be executed in the analysis part. 
The creation of the Database cannot be executed with the python script, so it was executed in the Postgres interface pgAdmin 4:
```
CREATE DATABASE data_engineer;
```
Schema and Table creation were generate inside the python file.

## Python Script
The Python script is organized as follows:
- main() function to orchestrate the entire process, containing all variables needed and the steps to be executed.
- Every step has its own function, with the corresponding docString explaining the purpose of it, and the parameters needed

## Database INI Script
File containing all information needed to connect to the Postgres database. This file MUST be in the .gitignore file for real projects, since it contains the information to access the database.

## Insights 
The dataset contains transactional data for a year period from a online retail. The invoice_number column provides important information about the transactions, since the ones that starts with letter 'c' indicates cancellation. For the analysis those cases were removed from the query. After doing the analysis, it become clear that the orders are well distributed among the products the company offers. The customers are a little more clusterized, with the top 2 clients having a total amount of purchases way higher than the others. Finally, the time series shows very clear that the monthly revenue has been growing over time, with a peak at november. It is important to highlight that the last december data is not complete, so not much information can be driven out of that month.

<h3 align="center">Sales Data Transformation and Aggregation </h3>

##  About
This project was created to analyze the data from a large sales dataset. First, a .csv file is read using spark dataframe. The data types were adjusted and the missing values replaced by 0 in the CustomerID column. With the data cleaned and the right data types, a few analysis were made using PySpark language and printed in the screen (just for the purpose of the exercise, real pipelines should avoid unnecessary actions in a spark pipeline). Finally, the dataset is written into a Postgres Database. 

## Project Requirements
The following instructions were received:
- Load the dataset into a PySpark DataFrame. 
- Perform necessary data cleaning (e.g., handle missing values, correct data types). 
- Calculate total sales and the number of transactions per day. 
- Identify the top 10 products with the highest sales. 
- Save the transformed data back to a SQL database. 

## Getting Started
Before executing the code, make sure that the necessary packages are installed, all files are in the same folder and the terminal is being executed in the same folder.
It is necessary to download the .csv file from the following link: https://www.kaggle.com/datasets/carrie1/ecommerce-data

The file should be saved in the same project directory and named 'data.csv'

## Python Script
The Python script is organized as follows:
- main() function to orchestrate the entire process, containing all variables needed and the steps to be executed.
- Every step has its own function, with the corresponding docString explaining the purpose of it, and the parameters needed

## Database INI Script
File containing all information needed to connect to the Postgres database. This file MUST be in the .gitignore file for real projects, since it contains the information to access the database.

## Insights 
The dataset contains transactional data for a year period from a online retail. The analysis done in this project enables a better visualization on the overall perfomance of the retail store. It is possible to infer that the annual revenue is close to 1 million, november was the month with the highest number of transactions, which also explains the peak of the monthly revenue, and the products bringing more sales amount to the store are Stock Codes DOT and 22423, with more than a hundred thousand each.

<h3 align="center">Complex SQL Queries for Data Exploration </h3>

##  About
This project was created to explore a database with some complex SQL queries. A lot of business questions were answered by those queries.

## Project Requirements
The following instructions were received:
- Load the database and create the necessary tables in an SQLite or PostgreSQL database setup.
- Write SQL queries to perform the following tasks
  - Find the top 5 most popular genres by total sales.
  - Calculate the average invoice total by country.
  - Identify the top 3 most valued customers based on the total sum of invoices.
  - Generate a report listing all employees who have sold over a specified amount (provide examples for amounts 1000,1000,5000).
- Export the results of each query to a CSV file.

## Getting Started
Before executing the queries, it is necessary to execute the scripts to create the assets: https://github.com/lerocha/chinook-database/releases

Since this project was developed using a Postgres database, the Chinook_PostgreSql.sql file was used.

## SQL Script
The SQL script contains all the queries to be executed to answer the questions mentioned above. For each one, it was added a comment with the question and .csv file generated.
All the scripts were executed in the Postgres interface pgAdmin 4 and the .csv files exported via the interface as well.
All the queries were created based on the data model provided in the repository provided: https://github.com/lerocha/chinook-database

### Generate a report listing all employees who have sold over a specified amount Query
This script prepares a query to receive a variable and generate the report based on that condition. To do so, a PREPARE function was used, expecting to receive a integer as parameter. Since the requirement was to provide examples based on a common filter, this approach was used to avoid code repetition. This way, to generate the report, the user only needs to execute the following command, with the filter example as 800:
```
EXECUTE report (800);
```

## Insights 
This project is based on the following data model, about a digital music store:
![image](https://github.com/user-attachments/assets/d482db84-eda7-40d9-ae04-7edd7671f5a4)
The analysis done in that data allow the following conclusions:
- The most popular genre with no doubt is rock, which doubles the total sales from the second place, which is latin music
- The country with the higher average invoices is Chile, but there are no big differences among all countries listed in the invoices, with the highest average being 6.66 and the lowest 5.37
- The three top consumers has a total amount pretty close to each other
- Out of 8 employees, only 3 has invoices and they all sold close amounts, with Jane having the highest amount and being the only one in the report when passing the 800 parameter as a filter. 
