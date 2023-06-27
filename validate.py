import snowflake.connector

# Gets the version of snowflake 
snowflake_connection = snowflake.connector.connect(
    user='VEDAMALIKA',
    password='9@Vedasai',
    account='qf67219.ap-southeast-1'
    )
cs = snowflake_connection.cursor()
try:
    cs.execute("SELECT current_version()")
    one_row = cs.fetchone()
    print(one_row[0])
finally:
    cs.close()
snowflake_connection.close()