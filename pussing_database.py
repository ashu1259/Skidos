from pyathena import connect
from pyathena.pandas_cursor import PandasCursor

def push_to_athena(data, database, table_name, region='your-aws-region'):
    # Replace 'your-aws-region' with your actual AWS region

    # AWS access and secret keys can be set as environment variables or directly provided here
    conn = connect(aws_access_key_id='your-access-key',
                   aws_secret_access_key='your-secret-key',
                   s3_staging_dir='s3://your-s3-bucket/path/',
                   region_name=region)

    # Create a cursor using a PandasCursor
    cursor = PandasCursor(conn)

    # Create the Athena table
    cursor.execute(f'''
        CREATE EXTERNAL TABLE IF NOT EXISTS {table_name} (
            `Event Name` STRING,
            `SDK version` STRING,
            `User ID` STRING,
            `Platform` STRING,
            `Current Game` STRING
        )
        ROW FORMAT DELIMITED
        FIELDS TERMINATED BY ','
        LOCATION 's3://your-s3-bucket/path/{table_name}/'
        TBLPROPERTIES ('skip.header.line.count'='1')
    ''')

    # Write the data to Athena
    cursor.execute(f"INSERT INTO {table_name} SELECT * FROM your_table_name")

    print(f"Data inserted into Athena table {table_name}")

# Example usage:
database_name = 'your_database_name'
athena_table_name = 'your_table_name'

push_to_athena(final_data, database_name, athena_table_name)
