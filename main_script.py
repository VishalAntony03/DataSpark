import pandas as pd
from mysql.connector import Error
from data_cleaning import clean_and_prepare_data, detect_encoding
from eda_analysis import eda_summary, analyze_customer_demographics, analyze_customer_purchases, analyze_sales_trends, analyze_products
from database_operations import create_table, insert_data, connect_to_mysql, close_mysql_connection

# File paths for the CSV files
file_paths = {
    "Customer": "D:/DataSpark Project/Data SET/Customers.csv",
    "Sales": "D:/DataSpark Project/Data SET/Sales.csv",
    "Products": "D:/DataSpark Project/Data SET/Products.csv",
    "Exchange_Rates": "D:/DataSpark Project/Data SET/Exchange_Rates.csv",
    "Data_Dictionary": "D:/DataSpark Project/Data SET/Data_Dictionary.csv"
}

data_frames = {}
for key, path in file_paths.items():
    try:
        encoding = detect_encoding(path)
        df = pd.read_csv(path, encoding=encoding)
        print(f"File read successfully with encoding {encoding}: {path}")

        # Rename columns to match SQL table schema if necessary
        if key == "Customer":
            df.rename(columns={
                "State Code": "State_Code",
                "Zip Code": "Zip_Code"
            }, inplace=True)
        elif key == "Sales":
            df.rename(columns={
                "Currency Code": "Currency_Code"
            }, inplace=True)
        elif key == "Products":
            df.rename(columns={
                "Unit Cost USD": "Unit_Cost_USD"
            }, inplace=True)
        elif key == "Data_Dictionary":
            df.rename(columns={
                "Table": "Table_Name",
                "Field": "Field_Name"
            }, inplace=True)

        data_frames[key] = clean_and_prepare_data(df, key)
        
        # Perform EDA for each dataframe
        eda_summary(df, key)
        if key == "Customer":
            analyze_customer_demographics(df)
        elif key == "Sales":
            analyze_customer_purchases(df)
            analyze_sales_trends(df)
        elif key == "Products":
            analyze_products(df)
        
    except Exception as e:
        print(f"Failed to read and process {key} data: {e}")

# Connect to MySQL
conn, cursor = connect_to_mysql()

try:
    if conn.is_connected():
        # Create tables in the correct order
        create_table("Products", cursor)
        create_table("Customer", cursor)
        create_table("Sales", cursor)  # Create Sales after Customer and Products
        create_table("Exchange_Rates", cursor)
        create_table("Data_Dictionary", cursor)
        
        for table_name, df in data_frames.items():
            insert_data(table_name, df, cursor)
            conn.commit()
    else:
        print("Connection Disconnected")

except Error as e:
    print(f"Error during database operations: {e}")

finally:
    close_mysql_connection(conn, cursor)
