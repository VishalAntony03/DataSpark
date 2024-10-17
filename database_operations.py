import mysql.connector
from mysql.connector import connect, Error
import pandas as pd

def connect_to_mysql():
    """
    Connect to MySQL database and return the connection and cursor.
    """
    conn = None
    cursor = None
    try:
        conn = mysql.connector.connect(
            host="localhost",
            user="root",
            password="vishal"
        )
        cursor = conn.cursor()
        # Create database if it does not exist
        cursor.execute("CREATE DATABASE IF NOT EXISTS data_spark")
        conn.database = 'data_spark'  # Select the database
        print("Connected to MySQL database")
    except Error as e:
        print(f"Error connecting to MySQL: {e}")
    return conn, cursor

def create_table(table_name, cursor):
    """
    Create table in MySQL if it does not exist.
    """
    create_table_sql = {
        "Customer": """
            CREATE TABLE IF NOT EXISTS Customer (
                CustomerKey INT PRIMARY KEY,
                Gender VARCHAR(10),
                Name VARCHAR(255),
                City VARCHAR(100),
                State_Code VARCHAR(50),
                State VARCHAR(100),
                Zip_Code VARCHAR(20),
                Country VARCHAR(100),
                Continent VARCHAR(100),
                Birthday DATE
            )
        """,
        "Sales": """
            CREATE TABLE IF NOT EXISTS Sales (
                `Order Number` INT PRIMARY KEY,
                `Line Item` INT,
                `Order Date` DATE,
                `Delivery Date` DATE,
                CustomerKey INT,
                StoreKey INT,
                ProductKey INT,
                Quantity INT,
                Currency_Code VARCHAR(10),
                FOREIGN KEY (CustomerKey) REFERENCES Customer(CustomerKey),
                FOREIGN KEY (ProductKey) REFERENCES Products(ProductKey)
            )
        """,
        "Products": """
            CREATE TABLE IF NOT EXISTS Products (
                ProductKey INT PRIMARY KEY,
                Product_Name VARCHAR(255),
                Brand VARCHAR(100),
                Color VARCHAR(50),
                Unit_Cost_USD DECIMAL(10, 2),
                Unit_Price_USD DECIMAL(10, 2),
                SubcategoryKey INT,
                Subcategory VARCHAR(100),
                CategoryKey INT,
                Category VARCHAR(100)
            )
        """,
        "Exchange_Rates": """
            CREATE TABLE IF NOT EXISTS Exchange_Rates (
                Date DATE,
                Currency VARCHAR(10),
                Exchange DECIMAL(10, 4),
                PRIMARY KEY (Date, Currency)
            )
        """,
        "Data_Dictionary": """
            CREATE TABLE IF NOT EXISTS Data_Dictionary (
                Table_Name VARCHAR(100),
                Field_Name VARCHAR(100),
                Description TEXT
            )
        """
    }
    create_table_sql_statement = create_table_sql.get(table_name, "")
    if create_table_sql_statement:
        try:
            cursor.execute(create_table_sql_statement)
            print(f"Table {table_name} created or already exists.")
        except Error as e:
            print(f"Error creating table {table_name}: {e}")

def insert_data(table_name, df, cursor):
    """
    Insert data into the specified table.
    """
    if not df.empty:
        columns = ', '.join([f"`{col}`" for col in df.columns])
        placeholders = ', '.join(['%s'] * len(df.columns))
        insert_sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
        data_tuples = [tuple(x) for x in df.to_numpy()]

        # Ensure no None values are inserted into the database
        data_tuples = [tuple(None if pd.isna(x) else x for x in row) for row in data_tuples]

        try:
            # Handle primary key duplicates
            if table_name in ["Sales", "Products", "Customer", "Exchange_Rates"]:
                primary_key_column = {
                    "Sales": 'Order Number',
                    "Products": 'ProductKey',
                    "Customer": 'CustomerKey',
                    "Exchange_Rates": 'Date-Currency'
                }.get(table_name)
                
                if primary_key_column:
                    existing_keys_query = f"SELECT `{primary_key_column}` FROM `{table_name}`"
                    cursor.execute(existing_keys_query)
                    existing_keys = {row[0] for row in cursor.fetchall()}

                    # Filter out rows with existing primary keys
                    if table_name == "Exchange_Rates":
                        df['Date-Currency'] = df['Date'].astype(str) + '-' + df['Currency']
                        existing_keys = set(row[0] for row in cursor.execute(f"SELECT CONCAT(Date, '-', Currency) FROM Exchange_Rates").fetchall())
                        df = df[~df['Date-Currency'].isin(existing_keys)]
                    else:
                        df = df[~df[primary_key_column].isin(existing_keys)]

                    if df.empty:
                        print(f"No new data to insert into {table_name}.")
                        return

            cursor.executemany(insert_sql, data_tuples)
            print(f"Data inserted into {table_name} successfully.")
        except Error as e:
            print(f"Error inserting data into {table_name}: {e}")
    else:
        print(f"No data to insert for {table_name}.")

def close_mysql_connection(conn, cursor):
    """
    Close MySQL connection and cursor.
    """
