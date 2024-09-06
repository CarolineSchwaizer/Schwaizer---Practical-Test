import matplotlib.pyplot as plt
import pandas as pd
import psycopg2
from psycopg2 import sql
from configparser import ConfigParser
from ucimlrepo import fetch_ucirepo 


def main():
    csv_file_path = 'data.csv'
    table_schema = 'retail'
    table_name = 'online_retail'
    sql_file_path = 'sql_queries.sql'

    df = read_retail_dataset()
    create_csv_file(df, csv_file_path)

    config = load_config()
    conn = connect(config)
    create_schema_if_not_exists(conn, table_schema)
    create_table_if_not_exists(conn, table_schema, table_name)
    copy_data_from_csv_to_table(conn, table_schema, table_name, csv_file_path)

    query_results = execute_sql_file_and_fetch_results(conn, sql_file_path)
    generate_visualizations(query_results)



def read_retail_dataset() -> pd.DataFrame:
    '''
    This function fetches the online retail dataset from the UCI Machine Learning Repository,
    performs several preprocessing steps on the data, and returns the processed DataFrame.

    The following preprocessing steps are applied:
    - Set the correct data types
    - Convert the 'InvoiceDate' column to datetime format 'YYYY-MM-DD HH:MM:SS'.
    - Fill missing values in the 'CustomerID' column with 0, convert the column to integer type, and then to string type.

    Returns:
        pd.DataFrame: The preprocessed DataFrame
    
    Notes:
        - This function uses the `fetch_ucirepo` method to obtain the dataset. Ensure that the method
          and necessary imports are available in your environment. It is necessary to install the package 
          pip3 install -U ucimlrepo
    '''
    online_retail = fetch_ucirepo(id=352) 
    df = online_retail.data.original
    
    print(df.dtypes)

    df['InvoiceNo'] = df['InvoiceNo'].astype(str)
    df['StockCode'] = df['StockCode'].astype(str)
    df['InvoiceDate'] = pd.to_datetime(df['InvoiceDate'], format='%m/%d/%Y %H:%M').dt.strftime('%Y-%m-%d %H:%M:%S')
    df['CustomerID'] = df['CustomerID'].fillna(0)
    df['CustomerID'] = df['CustomerID'].astype(int)
    df['CustomerID'] = df['CustomerID'].astype(str)
    df['Country'] = df['Country'].astype(str)
    
    return df


def create_csv_file(df: pd.DataFrame, file_path: str, mode: str = 'w') -> None:
    '''
    This function saves the provided pandas DataFrame to a CSV file at the specified location.
    It allows the user to specify the file path and write mode.

    Args:
        df (pd.DataFrame): The pandas DataFrame to be written to a CSV file.
        file_path (str): The path where the CSV file will be saved. This should include
            the file name and desired directory.
        mode (str, optional): The mode in which to write the CSV file. Options include:
            - 'w': Write mode (overwrite the file if it exists).
            - 'a': Append mode (append to the file if it exists).
            Defaults to 'w'.

    Returns:
        None
    '''
    df.to_csv(file_path, mode=mode, index=False)


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


def connect(config:dict):
    '''
    This function attempts to establish a connection to a PostgreSQL database server
    using the parameters provided in the `config` dictionary. If the connection is
    successful, it prints a confirmation message and returns the connection object.
    If there is an error during the connection process, it prints the error message
    and returns `None`.

    Args:
        config (dict): A dictionary containing the configuration parameters for the
            PostgreSQL connection. The dictionary should include keys such as host, 
            database, user, and password

    Returns:
        psycopg2.extensions.connection or None: The connection object if the connection
            is successful; otherwise, `None`.

    Raises:
        psycopg2.DatabaseError: If there is a database-related error during the connection attempt.
        Exception: If there is any other error during the connection attempt.
    '''
    try:
        conn = psycopg2.connect(**config)
        print('Connected to the PostgreSQL server.')
        return conn
    except (psycopg2.DatabaseError, Exception) as error:
        print(f"Error: {error}")
        return None


def execute_query(conn, query:str, fetch_result:bool = False):
    '''
    This function executes a given SQL query using the provided PostgreSQL database connection.
    It commits the transaction if the execution is successful. If the connection is `None`,
    or if an error occurs during execution, appropriate messages are printed.

    Args:
        conn (psycopg2.extensions.connection): The PostgreSQL database connection object.
        query (str): The SQL query to be executed.

    Returns:
        None

    Raises:
        psycopg2.Error: If there is an error during query execution.
    '''
    try:
        if conn is not None:
            cur = conn.cursor()
            cur.execute(query)
            conn.commit()
        else:
            print("Connection to PostgreSQL failed.")
    except psycopg2.Error as e:
        print(f"An error occurred: {e}")

    if fetch_result:
        return cur.fetchone()


def create_schema_if_not_exists(conn, table_schema:str) -> None:
    query = sql.SQL(f'CREATE SCHEMA IF NOT EXISTS {table_schema}')
    execute_query(conn, query)
    print(f'Schema {table_schema} created successfully or already exists!')

    
def create_table_if_not_exists(conn, table_schema:str, table_name:str) -> None:
    query = sql.SQL(f'''
        CREATE TABLE IF NOT EXISTS {table_schema}.{table_name} (
            InvoiceNo VARCHAR,
            StockCode VARCHAR,
            Description TEXT,
            Quantity INTEGER,
            InvoiceDate TIMESTAMP,
            UnitPrice FLOAT,
            CustomerID VARCHAR,
            Country VARCHAR
        )
    ''')
    execute_query(conn, query)
    print(f'Table {table_name} created successfully or already exists!')


def copy_data_from_csv_to_table(conn, table_schema:str, table_name:str, file_path:str) -> None:
    '''
    This function imports data from a specified CSV file into a PostgreSQL table. It first checks
    if the table already contains data by running a simple query. If the table is empty, it uses the
    `COPY` command to import data from the CSV file into the table. If the table already has data,
    it prints a message indicating that no import is performed.

    Args:
        conn (psycopg2.extensions.connection): The PostgreSQL database connection object.
        table_schema (str): The schema of the table where data will be imported.
        table_name (str): The name of the table where data will be imported.
        file_path (str): The path to the CSV file that contains the data to be imported.

    Returns:
        None
    '''
    test_query = f"SELECT 1 FROM {table_schema}.{table_name}"
    test_result = execute_query(conn, test_query, fetch_result=True)

    if test_result is None:
        query = f'''
                    COPY {table_schema}.{table_name} FROM stdin WITH CSV HEADER
                    DELIMITER as ','
                '''
        with open(file_path, 'r') as f:
            cur = conn.cursor()
            cur.copy_expert(sql=query, file=f)
            conn.commit()
            cur.close()
        print(f"Data imported successfully into table '{table_name}'!")
    else:
        print(f'Table {table_name} already has data')
    
    
def execute_sql_file_and_fetch_results(conn, file_path:str):
    '''
    This function reads SQL commands from a specified file, executes them sequentially on the 
    provided PostgreSQL database connection, and fetches the results of each query. It handles
    multiple queries in a single file, separated by semicolons. The results for each query are 
    returned as a list of tuples, where each tuple contains the column names and the result rows.

    Args:
        conn (psycopg2.extensions.connection): The PostgreSQL database connection object.
        file_path (str): The path to the file containing the SQL commands to be executed.

    Returns:
        List[Tuple[List[str], List[Tuple[Any]]]]: A list of tuples, where each tuple represents
        the results of a query. Each tuple contains:
            - A list of column names.
            - A list of rows, where each row is a tuple of column values.
    '''
    with open(file_path, 'r') as file:
        sql_script = file.read()

    query_results = []

    with conn.cursor() as cur:
        queries = sql_script.split(';')
        for query in queries:
            query = query.strip()
            if query:
                cur.execute(query)
                if cur.description:
                    result = cur.fetchall()
                    columns = [desc[0] for desc in cur.description]
                    query_results.append((columns, result))
    return query_results
        

def prepare_result(query_result:tuple):
    '''
    This function takes the result of a SQL query, which is expected to be a tuple containing 
    column headers and records. It processes the records to separate and convert specific columns 
    into lists of IDs and amounts. The amounts are converted to float type.

    Args:
        query_result (tuple): A tuple representing the result of a SQL query. It should contain:
            - headers (list of str): A list of column names from the query result.
            - records (list of tuples): A list of rows, where each row is a tuple of column values.

    Returns:
        - id (list of int): A list of IDs extracted from the query result.
        - amount (list of float): A list of amounts extracted from the query result, converted to float.
    '''
    headers, records = query_result
    id, amounts = zip(*records)
    amount = [float(amount) for amount in amounts]
    return id, amount


def generate_visualizations(query_results):
    '''
    This function takes query results, processes them to create visualizations, and saves the 
    visualizations as PNG files. It generates three types of visualizations:
    1. A bar chart of total purchase amounts per customer.
    2. A pie chart and a bar chart of the top 10 stock codes by total orders.
    3. A line plot of monthly revenue over time.

    Args:
        query_results (list of tuples): A list containing the results of SQL queries. It should have:
            - query_results[0] (tuple): A tuple with headers and records for customer purchase amounts.
            - query_results[1] (tuple): A tuple with headers and records for stock codes and total orders.
            - query_results[2] (tuple): A tuple with headers and records for monthly revenue data, including 'year' and 'month'.

    Returns:
        None
    '''
    customer_ids, total_purchase_amounts = prepare_result(query_results[0])
    
    plt.figure(figsize=(12, 8))
    plt.bar(customer_ids, total_purchase_amounts, color='skyblue')
    plt.title('Total Purchase Amount per Customer')
    plt.xlabel('Customer ID')
    plt.ylabel('Total Purchase Amount')
    plt.xticks(rotation=45)

    plt.savefig('total_purchase_amount_per_customer.png')


    stock_codes, total_orders = prepare_result(query_results[1])
    
    total_orders = list(total_orders)
    top_n = 10
    top_indices = sorted(range(len(total_orders)), key=lambda i: total_orders[i], reverse=True)[:top_n]
    top_stock_codes = [stock_codes[i] for i in top_indices]
    top_total_orders = [total_orders[i] for i in top_indices]

    plt.figure(figsize=(14, 6))
    plt.subplot(1, 2, 1)
    plt.pie(top_total_orders, labels=top_stock_codes, autopct='%1.1f%%', startangle=140, colors=plt.cm.Paired(range(top_n)))
    plt.title('Top 10 Stock Codes by Orders')

    plt.subplot(1, 2, 2)
    plt.bar(top_stock_codes, top_total_orders, color='skyblue')
    plt.title('Top 10 Stock Codes by Orders')
    plt.xlabel('Stock Code')
    plt.ylabel('Total Orders')
    plt.xticks(rotation=45)

    plt.savefig('stock_code_orders_comparison.png')


    columns, data = query_results[2]
    df = pd.DataFrame(data, columns=columns)
    df['monthly_revenue'] = df['monthly_revenue'].astype(float)
    df['date'] = pd.to_datetime(df[['year', 'month']].assign(day=1))
    df.set_index('date', inplace=True)
    df.sort_index(inplace=True)

    plt.figure(figsize=(12, 6))
    plt.plot(df.index, df['monthly_revenue'], marker='o', linestyle='-')
    plt.title('Monthly Revenue Time Series')
    plt.xlabel('Date')
    plt.ylabel('Monthly Revenue')
    plt.grid(True)
    plt.xticks(rotation=45)
    plt.tight_layout()

    plt.savefig('monthly_revenue_time_series.png')

main()