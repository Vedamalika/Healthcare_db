import mysql.connector

# MySQL configuration
mysql_config = {
    'user': 'root',
    'password': 'Kunjara123@',
    'host': 'localhost',
}

# Connect to MySQL server
try:
    mysql_conn = mysql.connector.connect(**mysql_config)
    print('Connected to MySQL server successfully.')
except mysql.connector.Error as error:
    print('Failed to connect to MySQL server:', error)
    exit(1)

# Execute SHOW VARIABLES command to retrieve binary logging settings
query = 'SHOW VARIABLES LIKE "log_bin"'
try:
    cursor = mysql_conn.cursor()
    cursor.execute(query)
    result = cursor.fetchone()
    variable_name, log_bin_value = result
    print('Binary logging is enabled:', log_bin_value)
except mysql.connector.Error as error:
    print('Failed to execute query:', error)

# Close the connection
mysql_conn.close()


import mysql.connector

# MySQL configuration
mysql_config = {
    'user': 'root',
    'password': 'Kunjara123@',
    'host': 'localhost',
}

# Connect to MySQL server
try:
    mysql_conn = mysql.connector.connect(**mysql_config)
    print('Connected to MySQL server successfully.')
except mysql.connector.Error as error:
    print('Failed to connect to MySQL server:', error)
    exit(1)

# Execute SHOW VARIABLES command to retrieve replication settings
query = 'SHOW VARIABLES LIKE "server_id"'
try:
    cursor = mysql_conn.cursor()
    cursor.execute(query)
    result = cursor.fetchone()
    variable_name, server_id_value = result
    print('Server ID:', server_id_value)
except mysql.connector.Error as error:
    print('Failed to execute query:', error)

# Close the connection
mysql_conn.close()


from mysql.connector import connect
from mysql.connector.errors import Error
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import (
    WriteRowsEvent,
    UpdateRowsEvent,
    DeleteRowsEvent
)

# MySQL configuration
mysql_config = {
    'user': 'root',
    'password': 'Kunjara123@',
    'host': 'localhost',
    'database': 'Automobile',
}

# Callback function to process binlog events
def process_event(event):
    if isinstance(event, (WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent)):
        print(event)

# Connect to MySQL server
try:
    mysql_conn = connect(**mysql_config)
    print('Connected to MySQL server successfully.')
except Error as error:
    print('Failed to connect to MySQL server:', error)
    exit(1)

# Create binlog stream reader
stream = BinLogStreamReader(
    connection_settings=mysql_config,
    server_id=1,  # Replace with the actual server ID of the MySQL instance
    blocking=True,
    only_events=[WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent]
)

# Start reading binlog events
for binlogevent in stream:
    process_event(binlogevent)

# Close the stream and connection
stream.close()
mysql_conn.close()


import mysql.connector
from mysql.connector import Error
from mysql.connector.constants import ClientFlag
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import (
    UpdateRowsEvent,
    WriteRowsEvent,
    DeleteRowsEvent,
)
import snowflake.connector

# MySQL configuration
mysql_config = {
    'user': 'root',
    'password': 'Kunjara123@',
    'host': 'localhost',
    'database': 'Automobile',
}

# Snowflake configuration
snowflake_config = {
    'user': 'VEDAMALIKA',
    'password': '9@Vedasai',
    'account': 'qf67219.ap-southeast-1',
    'warehouse': 'Automobile_db',
    'database': 'Automobile.db',
    'schema': 'PUBLIC',
    'log_level': 'DEBUG'
}

# Connecting to MySQL
try:
    mysql_connection = mysql.connector.connect(**mysql_config)
    mysql_cursor = mysql_connection.cursor()
    print("Connected to MySQL")
except Error as e:
    print("MySQL Connection Error: ", e)

# Connecting to Snowflake
try:
    snowflake_connection = snowflake.connector.connect(**snowflake_config)
    snowflake_cursor = snowflake_connection.cursor()
    print("Connected to Snowflake")
except Error as e:
    print("Snowflake Connection Error: ", e)

def process_event(event):
    if isinstance(event, UpdateRowsEvent):
        for row in event.rows:
            table_name = event.table
            updated_data = row["after_values"]
            
            if table_name == "sales":
                # Map updated data to Snowflake table columns
                sales_id = updated_data["sales_id"]
                customer_name = updated_data["customer_name"]
                car_model = updated_data["car_model"]
                purchase_date = updated_data["purchase_date"]
                purchase_amount = updated_data["purchase_amount"]
                
                # Perform UPDATE operation in Snowflake
                update_query = """
                UPDATE sales
                SET customer_name = %s,
                    car_model = %s,
                    purchase_date = %s,
                    purchase_amount = %s
                WHERE sales_id = %s
                """
                snowflake_cursor.execute(update_query, (customer_name, car_model, purchase_date, purchase_amount, sales_id))
                snowflake_connection.commit()

            # Implement the same logic for other tables (insurance, finance, Crm, hr, it_department)

    elif isinstance(event, WriteRowsEvent):
        for row in event.rows:
            table_name = event.table
            inserted_data = row["values"]
            
            if table_name == "sales":
                # Map inserted data to Snowflake table columns
                sales_id = inserted_data["sales_id"]
                customer_name = inserted_data["customer_name"]
                car_model = inserted_data["car_model"]
                purchase_date = inserted_data["purchase_date"]
                purchase_amount = inserted_data["purchase_amount"]
                
                # Perform INSERT operation in Snowflake
                insert_query = """
                INSERT INTO sales (sales_id, customer_name, car_model, purchase_date, purchase_amount)
                VALUES (%s, %s, %s, %s, %s)
                """
                snowflake_cursor.execute(insert_query, (sales_id, customer_name, car_model, purchase_date, purchase_amount))
                snowflake_connection.commit()

            # Implement the same logic for other tables (insurance, finance, Crm, hr, it_department)

    elif isinstance(event, DeleteRowsEvent):
        for row in event.rows:
            table_name = event.table
            deleted_data = row["values"]
            
            if table_name == "sales":
                # Map deleted data to Snowflake table columns
                sales_id = deleted_data["sales_id"]
                
                # Perform DELETE operation in Snowflake
                delete_query = "DELETE FROM sales WHERE sales_id = %s"
                snowflake_cursor.execute(delete_query, (sales_id,))
                snowflake_connection.commit()

            # Implement the same logic for other tables (insurance, finance, Crm, hr, it_department)

# MySQL binlog stream reader
stream = BinLogStreamReader(
    connection_settings=mysql_config,
    server_id=1,
    blocking=True,
    only_events=[UpdateRowsEvent, WriteRowsEvent, DeleteRowsEvent],
    resume_stream=True,
)

for binlogevent in stream:
    for row in binlogevent.rows:
        event = {"table": binlogevent.table, "values": row}
        process_event(event)

# Closing connections
mysql_cursor.close()
mysql_connection.close()
snowflake_cursor.close()
snowflake_connection.close()
