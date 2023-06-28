import snowflake.connector
import sqlite3
from snowflake.connector.errors import ProgrammingError

def create_sqlite_connection():
    try:
        conn_sqlite = sqlite3.connect('Automobile.db')
        print("SQLite connection created")
        return conn_sqlite

    except sqlite3.Error as e:
        return e


# Creating the SQLite connection
conn_sqlite = create_sqlite_connection()

def create_tables_sqlite(conn):
    try:
        cursor_sqlite = conn.cursor()

        create_table1_query = """
        CREATE TABLE sales (
            sales_id INTEGER,
            customer_name string,
            car_model string,
            purchase_date date,
            purchase_amount float
        )
        """
        cursor_sqlite.execute(create_table1_query)
        print("Table 'sales' is created successfully")

        create_table2_query = """
        CREATE TABLE insurance (
            policy_id INTEGER,
            customer_name string,
            car_model string,
            policy_type string,
            premium_amount float
        )
        """
        cursor_sqlite.execute(create_table2_query)
        print("Table 'insurance' is created successfully")

        create_table3_query = """
        CREATE TABLE finance (
            finance_id INTEGER,
            customer_name string,
            car_model string,
            loan_amount float,
            interest_rate float
        )
        """
        cursor_sqlite.execute(create_table3_query)
        print("Table 'finance' is created successfully")

        create_table4_query = """
        CREATE TABLE Crm (
            customer_id INTEGER,
            customer_name string,
            email string,
            phone_number string,
            city string
        )
        """
        cursor_sqlite.execute(create_table4_query)
        print("Table 'Crm' is created successfully")

        create_table5_query = """
        CREATE TABLE hr (
            employee_id INTEGER,
            employee_name string,
            department string,
            position string,
            hire_date date
        )
        """
        cursor_sqlite.execute(create_table5_query)
        print("Table 'hr' is created successfully")

        create_table6_query = """
        CREATE TABLE it_department (
            ticket_id INTEGER,
            issue string,
            priority string,
            assigned_to string,
            status string
        )
        """
        cursor_sqlite.execute(create_table6_query)
        print("Table 'it_department' is created successfully")

        conn.commit()

    except sqlite3.Error as e:
        return e 

    except Exception as e:
        return e 

    
    cursor_sqlite.close()

    conn.close()

# calling the function for Creating the tables in SQLite
create_tables_sqlite(conn_sqlite)




# Snowflake connection parameters
snowflake_account = 'qf67219.ap-southeast-1'
snowflake_user = 'VEDAMALIKA'
snowflake_password = '9@Vedasai'
snowflake_warehouse = 'Automobile_db'
snowflake_database = 'Automobile'
snowflake_schema = 'PUBLIC'

sqlite_db_file = '/home/kunjara/Documents/Automobile_db/Automobile.db'

# def retrieve_data_from_snowflake():
#     conn_snowflake = snowflake.connector.connect(
#         user=snowflake_user,
#         password=snowflake_password,
#         account=snowflake_account,
#         warehouse=snowflake_warehouse,
#         database=snowflake_database,
#         schema=snowflake_schema
#     )

#     cursor_snowflake = conn_snowflake.cursor()

#     select_query = 'SELECT * FROM insurance'

#     try:
#         cursor_snowflake.execute(select_query)
#         data = cursor_snowflake.fetchall()
#         print("Data retrieved from Snowflake successfully.")
#     except ProgrammingError as e:
#         print(f"Error retrieving data from Snowflake: {e}")
#         cursor_snowflake.close()
#         conn_snowflake.close()
#         return None

#     cursor_snowflake.close()
#     conn_snowflake.close()
#     return data

# def insert_data_into_sqlite(data):
#     if data is None:
#         return

#     conn_sqlite = sqlite3.connect(sqlite_db_file)
#     cursor_sqlite = conn_sqlite.cursor()

#     insert_query_sqlite = '''
#     INSERT INTO insurance (policy_id, customer_name, car_model, policy_type, premium_amount)
#     VALUES (?, ?, ?, ?, ?)
#     '''

#     try:
#         for row in data:
#             cursor_sqlite.execute(insert_query_sqlite, row)

#         conn_sqlite.commit()
#         print("Data inserted into SQLite successfully.")
#     except sqlite3.Error as e:
#         print(f"Error inserting data into SQLite: {e}")

#     cursor_sqlite.close()
#     conn_sqlite.close()

# # Retrieve data from Snowflake
# data = retrieve_data_from_snowflake()

# # Insert data into SQLite
# insert_data_into_sqlite(data)



# def retrieve_data_from_snowflake():
#     conn_snowflake = snowflake.connector.connect(
#         user=snowflake_user,
#         password=snowflake_password,
#         account=snowflake_account,
#         warehouse=snowflake_warehouse,
#         database=snowflake_database,
#         schema=snowflake_schema
#     )

#     cursor_snowflake = conn_snowflake.cursor()

#     select_query = 'SELECT * FROM finance'

#     try:
#         cursor_snowflake.execute(select_query)
#         data = cursor_snowflake.fetchall()
#         print("Data retrieved from Snowflake successfully.")
#     except ProgrammingError as e:
#         print(f"Error retrieving data from Snowflake: {e}")
#         cursor_snowflake.close()
#         conn_snowflake.close()
#         return None

#     cursor_snowflake.close()
#     conn_snowflake.close()
#     return data

# def insert_data_into_sqlite(data):
#     if data is None:
#         return

#     conn_sqlite = sqlite3.connect(sqlite_db_file)
#     cursor_sqlite = conn_sqlite.cursor()

#     insert_query_sqlite = '''
#     INSERT INTO finance (finance_id, customer_name, car_model, loan_amount, interest_rate)
#     VALUES (?, ?, ?, ?, ?)
#     '''

#     try:
#         for row in data:
#             cursor_sqlite.execute(insert_query_sqlite, row)

#         conn_sqlite.commit()
#         print("Data inserted into SQLite successfully.")
#     except sqlite3.Error as e:
#         print(f"Error inserting data into SQLite: {e}")

#     cursor_sqlite.close()
#     conn_sqlite.close()

# # Retrieve data from Snowflake
# data = retrieve_data_from_snowflake()

# # Insert data into SQLite
# insert_data_into_sqlite(data)




# def retrieve_data_from_snowflake():
#     conn_snowflake = snowflake.connector.connect(
#         user=snowflake_user,
#         password=snowflake_password,
#         account=snowflake_account,
#         warehouse=snowflake_warehouse,
#         database=snowflake_database,
#         schema=snowflake_schema
#     )

#     cursor_snowflake = conn_snowflake.cursor()

#     select_query = 'SELECT * FROM Crm'

#     try:
#         cursor_snowflake.execute(select_query)
#         data = cursor_snowflake.fetchall()
#         print("Data retrieved from Snowflake successfully.")
#     except ProgrammingError as e:
#         print(f"Error retrieving data from Snowflake: {e}")
#         cursor_snowflake.close()
#         conn_snowflake.close()
#         return None

#     cursor_snowflake.close()
#     conn_snowflake.close()
#     return data

# def insert_data_into_sqlite(data):
#     if data is None:
#         return

#     conn_sqlite = sqlite3.connect(sqlite_db_file)
#     cursor_sqlite = conn_sqlite.cursor()

#     insert_query_sqlite = '''
#     INSERT INTO Crm (customer_id, customer_name, email, phone_number, city)
#     VALUES (?, ?, ?, ?, ?)
#     '''

#     try:
#         for row in data:
#             cursor_sqlite.execute(insert_query_sqlite, row)

#         conn_sqlite.commit()
#         print("Data inserted into SQLite successfully.")
#     except sqlite3.Error as e:
#         print(f"Error inserting data into SQLite: {e}")

#     cursor_sqlite.close()
#     conn_sqlite.close()

# # Retrieve data from Snowflake
# data = retrieve_data_from_snowflake()

# # Insert data into SQLite
# insert_data_into_sqlite(data)



# def retrieve_data_from_snowflake():
#     conn_snowflake = snowflake.connector.connect(
#         user=snowflake_user,
#         password=snowflake_password,
#         account=snowflake_account,
#         warehouse=snowflake_warehouse,
#         database=snowflake_database,
#         schema=snowflake_schema
#     )

#     cursor_snowflake = conn_snowflake.cursor()

#     select_query = 'SELECT * FROM hr'

#     try:
#         cursor_snowflake.execute(select_query)
#         data = cursor_snowflake.fetchall()
#         print("Data retrieved from Snowflake successfully.")
#     except ProgrammingError as e:
#         print(f"Error retrieving data from Snowflake: {e}")
#         cursor_snowflake.close()
#         conn_snowflake.close()
#         return None

#     cursor_snowflake.close()
#     conn_snowflake.close()
#     return data

# def insert_data_into_sqlite(data):
#     if data is None:
#         return

#     conn_sqlite = sqlite3.connect(sqlite_db_file)
#     cursor_sqlite = conn_sqlite.cursor()

#     insert_query_sqlite = '''
#     INSERT INTO hr (employee_id, employee_name, department, position, hire_date)
#     VALUES (?, ?, ?, ?, ?)
#     '''

#     try:
#         for row in data:
#             cursor_sqlite.execute(insert_query_sqlite, row)

#         conn_sqlite.commit()
#         print("Data inserted into SQLite successfully.")
#     except sqlite3.Error as e:
#         print(f"Error inserting data into SQLite: {e}")

#     cursor_sqlite.close()
#     conn_sqlite.close()

# # Retrieve data from Snowflake
# data = retrieve_data_from_snowflake()

# # Insert data into SQLite
# insert_data_into_sqlite(data)


# def retrieve_data_from_snowflake():
#     conn_snowflake = snowflake.connector.connect(
#         user=snowflake_user,
#         password=snowflake_password,
#         account=snowflake_account,
#         warehouse=snowflake_warehouse,
#         database=snowflake_database,
#         schema=snowflake_schema
#     )

#     cursor_snowflake = conn_snowflake.cursor()

#     select_query = 'SELECT * FROM it_department'

#     try:
#         cursor_snowflake.execute(select_query)
#         data = cursor_snowflake.fetchall()
#         print("Data retrieved from Snowflake successfully.")
#     except ProgrammingError as e:
#         print(f"Error retrieving data from Snowflake: {e}")
#         cursor_snowflake.close()
#         conn_snowflake.close()
#         return None

#     cursor_snowflake.close()
#     conn_snowflake.close()
#     return data

# def insert_data_into_sqlite(data):
#     if data is None:
#         return

#     conn_sqlite = sqlite3.connect(sqlite_db_file)
#     cursor_sqlite = conn_sqlite.cursor()

#     insert_query_sqlite = '''
#     INSERT INTO it_department (ticket_id, issue, priority, assigned_to, status)
#     VALUES (?, ?, ?, ?, ?)
#     '''

#     try:
#         for row in data:
#             cursor_sqlite.execute(insert_query_sqlite, row)

#         conn_sqlite.commit()
#         print("Data inserted into SQLite successfully.")
#     except sqlite3.Error as e:
#         print(f"Error inserting data into SQLite: {e}")

#     cursor_sqlite.close()
#     conn_sqlite.close()

# # Retrieve data from Snowflake
# data = retrieve_data_from_snowflake()

# # Insert data into SQLite
# insert_data_into_sqlite(data)



def retrieve_data_from_snowflake():
    conn_snowflake = snowflake.connector.connect(
        user=snowflake_user,
        password=snowflake_password,
        account=snowflake_account,
        warehouse=snowflake_warehouse,
        database=snowflake_database,
        schema=snowflake_schema
    )

    cursor_snowflake = conn_snowflake.cursor()

    select_query = 'SELECT * FROM sales'

    try:
        cursor_snowflake.execute(select_query)
        data = cursor_snowflake.fetchall()
        print("Data retrieved from Snowflake successfully.")
    except ProgrammingError as e:
        print(f"Error retrieving data from Snowflake: {e}")
        cursor_snowflake.close()
        conn_snowflake.close()
        return None

    cursor_snowflake.close()
    conn_snowflake.close()
    return data

def insert_data_into_sqlite(data):
    if data is None:
        return

    conn_sqlite = sqlite3.connect(sqlite_db_file)
    cursor_sqlite = conn_sqlite.cursor()

    insert_query_sqlite = '''
    INSERT INTO sales (sales_id, customer_name, car_model, purchase_date, purchase_amount)
    VALUES (?, ?, ?, ?, ?)
    '''

    try:
        for row in data:
            cursor_sqlite.execute(insert_query_sqlite, row)

        conn_sqlite.commit()
        print("Data inserted into SQLite successfully.")
    except sqlite3.Error as e:
        print(f"Error inserting data into SQLite: {e}")

    cursor_sqlite.close()
    conn_sqlite.close()

# Retrieve data from Snowflake
data = retrieve_data_from_snowflake()

# Insert data into SQLite
insert_data_into_sqlite(data)