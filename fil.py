# import mysql.connector

# # MySQL configuration
# mysql_config = {
#     'user': 'root',
#     'password': 'Kunjara123@',
#     'host': 'localhost',
# }

# # Connect to MySQL server
# try:
#     mysql_conn = mysql.connector.connect(**mysql_config)
#     print('Connected to MySQL server successfully.')
# except mysql.connector.Error as error:
#     print('Failed to connect to MySQL server:', error)
#     exit(1)

# # Execute SHOW VARIABLES command to retrieve binary logging settings
# query = 'SHOW VARIABLES LIKE "log_bin"'
# try:
#     cursor = mysql_conn.cursor()
#     cursor.execute(query)
#     result = cursor.fetchone()
#     variable_name, log_bin_value = result
#     print('Binary logging is enabled:', log_bin_value)
# except mysql.connector.Error as error:
#     print('Failed to execute query:', error)

# # Close the connection
# mysql_conn.close()


# import mysql.connector

# # MySQL configuration
# mysql_config = {
#     'user': 'root',
#     'password': 'Kunjara123@',
#     'host': 'localhost',
# }

# # Connect to MySQL server
# try:
#     mysql_conn = mysql.connector.connect(**mysql_config)
#     print('Connected to MySQL server successfully.')
# except mysql.connector.Error as error:
#     print('Failed to connect to MySQL server:', error)
#     exit(1)

# # Execute SHOW VARIABLES command to retrieve replication settings
# query = 'SHOW VARIABLES LIKE "server_id"'
# try:
#     cursor = mysql_conn.cursor()
#     cursor.execute(query)
#     result = cursor.fetchone()
#     variable_name, server_id_value = result
#     print('Server ID:', server_id_value)
# except mysql.connector.Error as error:
#     print('Failed to execute query:', error)

# # Close the connection
# mysql_conn.close()


# from mysql.connector import connect
# from mysql.connector.errors import Error
# from pymysqlreplication import BinLogStreamReader
# from pymysqlreplication.row_event import (
#     WriteRowsEvent,
#     UpdateRowsEvent,
#     DeleteRowsEvent
# )

# # MySQL configuration
# mysql_config = {
#     'user': 'root',
#     'password': 'Kunjara123@',
#     'host': 'localhost',
#     'database': 'Automobile',
# }

# # Callback function to process binlog events
# def process_event(event):
#     if isinstance(event, (WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent)):
#         print(event)

# # Connect to MySQL server
# try:
#     mysql_conn = connect(**mysql_config)
#     print('Connected to MySQL server successfully.')
# except Error as error:
#     print('Failed to connect to MySQL server:', error)
#     exit(1)

# # Create binlog stream reader
# stream = BinLogStreamReader(
#     connection_settings=mysql_config,
#     server_id=1,  # Replace with the actual server ID of the MySQL instance
#     blocking=True,
#     only_events=[WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent]
# )

# # Start reading binlog events
# for binlogevent in stream:
#     process_event(binlogevent)

# # Close the stream and connection
# stream.close()
# mysql_conn.close()
