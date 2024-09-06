from configparser import ConfigParser
from pyspark.sql import functions as F
from pyspark.sql import SparkSession, DataFrame

def main():
    spark = SparkSession.builder.getOrCreate() 

    csv_file_path = "data.csv"
    db_table_name = 'sales'

    df = read_csv_file(spark, csv_file_path)
    calculate_total_sales(df)
    calculate_transaction_per_day(df)
    calculate_top_10_products(df)

    config = load_config()
    write_to_postgres(df, config, db_table_name)


def read_csv_file(spark, file_path:str) -> DataFrame:
    '''
    This function reads a CSV file into a Spark DataFrame, performs basic data processing,
    and returns the processed DataFrame. It infers the schema, handles missing values, and
    performs type casting and timestamp formatting.

    Args:
        spark (SparkSession): The Spark session object used to read the CSV file.
        file_path (str): The path to the CSV file to be read.

    Returns:
        DataFrame: A Spark DataFrame containing the processed data from the CSV file.
    '''
    df = spark.read.csv(file_path, header=True, inferSchema=True)
    df.printSchema()

    df = df.fillna({'CustomerID': 0})
    df = df.select(
        F.col('InvoiceNo')
        ,F.col('StockCode')
        ,F.col('Description')
        ,F.col('Quantity')
        ,F.to_timestamp("InvoiceDate", "M/d/yyyy H:mm").alias('InvoiceDate')
        ,F.col('UnitPrice')
        ,F.col('CustomerID').cast('string')
        ,F.col('Country')
    )
    return df


def calculate_total_sales(df:DataFrame) -> None:
    '''
    This function calculates the total sales by summing the product of 'Quantity' and 'UnitPrice'
    for all records in the DataFrame. The result is cast to a decimal with precision and scale
    to ensure accuracy. The total sales are then printed to the console.

    Args:
        df (DataFrame): A Spark DataFrame containing sales data with 'Quantity' and 'UnitPrice' columns.

    Returns:
        None
    '''
    df = df.groupBy().agg(
        F.sum(F.col('Quantity') * F.col('UnitPrice')).cast('decimal(26,2)').alias('total_sales')
    )
    print(f'Total sales in the period: ${df.collect()[0][0]}')


def calculate_transaction_per_day(df:DataFrame) -> DataFrame:
    '''
    This function groups the data by day, counts the number of distinct 'InvoiceNo' for each day,
    and returns a DataFrame with the total number of transactions per day. The result is ordered
    by the total number of transactions in descending order to be printed in the screen just for 
    this exercise, real pipelines should NOT show the results.

    Args:
        df (DataFrame): A Spark DataFrame containing sales data with an 'InvoiceDate' column (timestamp or date) 
            and 'InvoiceNo' column (transaction identifier).

    Returns:
        DataFrame: A Spark DataFrame with two columns:
            - 'InvoiceDate' (date): The date of the transactions.
            - 'total_transactions' (int): The number of distinct transactions for that day.
    '''
    df = df.groupBy(
        F.to_date(F.col('InvoiceDate')).alias('InvoiceDate')
    ).agg(
        F.countDistinct(F.col('InvoiceNo')).alias('total_transactions')
    )
    df.orderBy(F.col('total_transactions').desc()).show()
    return df


def calculate_top_10_products(df:DataFrame) -> DataFrame:
    '''
    This function groups the data by 'StockCode', calculates the total sales for each product by 
    summing the product of 'Quantity' and 'UnitPrice', and returns a DataFrame containing the top 10 
    products with the highest total sales. The result is ordered by total sales in descending order 
    and printed in the screen just for this exercise, real pipelines should NOT show the results.

    Args:
        df (DataFrame): A Spark DataFrame containing sales data with 'StockCode', 'Quantity', and 'UnitPrice' columns.

    Returns:
        DataFrame: A Spark DataFrame with two columns:
            - 'StockCode' (string): The code of the product.
            - 'total_sales' (decimal): The total sales amount for the product, cast to decimal(26,2).
    '''
    df = df.groupBy(
        F.col('StockCode')
    ).agg(
        F.sum(F.col('Quantity') * F.col('UnitPrice')).cast('decimal(26,2)').alias('total_sales')
    )

    df = df.orderBy(F.col('total_sales').desc()).limit(10)
    df.show()
    return df


def load_config(filename='database.ini', section='postgresql') -> dict:
    '''
    This function reads an .ini configuration file and extracts the parameters from
    a specified section. The parameters are returned as a dictionary where the keys
    are the parameter names and the values are the parameter values.

    Args:
        filename (str): The path to the .ini configuration file. Defaults to 'database.ini'.
        section (str): The section within the configuration file from which to read the parameters. 
            Defaults to 'postgresql'.

    Returns:
        dict: A dictionary containing the configuration parameters from the specified section.

    Raises:
        Exception: If the specified section is not found in the configuration file.
    '''
    parser = ConfigParser()
    parser.read(filename)

    config = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            config[param[0]] = param[1]
    else:
        raise Exception(f"Section {section} not found in the {filename} file")
    return config


def create_connection_string(hostname:str, port:str, database:str) -> str:
    return f'jdbc:postgresql://{hostname}:{port}/{database}'


def write_to_postgres(df:DataFrame, config:dict, table_name:str, mode:str = 'overwrite') -> None:
    '''
    This function writes the provided DataFrame to a PostgreSQL database table. The DataFrame is written 
    using JDBC with the connection details provided in the configuration dictionary. The `mode` parameter 
    is set to "overwrite", which means that any existing data in the target table will be replaced.

    Args:
        df (DataFrame): The Spark DataFrame to be written to the PostgreSQL table.
        config (dict): A dictionary containing the connection configuration for PostgreSQL with the following keys:
            - 'user' (str): The PostgreSQL username.
            - 'password' (str): The PostgreSQL password.
            - 'host' (str): The hostname or IP address of the PostgreSQL server.
            - 'port' (int): The port number of the PostgreSQL server (default is 5432).
            - 'database' (str): The name of the PostgreSQL database.
            - 'driver' (str): The JDBC driver class name (e.g., 'org.postgresql.Driver').
        table_name (str): The name of the PostgreSQL table where the DataFrame will be written.

    Returns:
        None
    '''
    properties = {
        'user': config['user'],
        'password': config['password'],
        'driver': 'org.postgresql.Driver'
    }

    url = create_connection_string(config['host'], config['port'], config['database'])
    df.write.jdbc(url=url, table=table_name, mode=mode, properties=properties)


main()