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


# Snowflake connection parameters
snowflake_account = 'qf67219.ap-southeast-1'
snowflake_user = 'VEDAMALIKA'
snowflake_password = '9@Vedasai'
snowflake_warehouse = 'Automobile_db'
snowflake_database = 'Automobile'
snowflake_schema = 'PUBLIC'

sqlite_db_file = '/home/kunjara/Documents/Automobile_db/Automobile.db'

#1 copying data to insurance table
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

    select_query = 'SELECT * FROM insurance'

    try:
        cursor_snowflake.execute(select_query)
        data = cursor_snowflake.fetchall()
        print("Data retrieved from Snowflake successfully.")
        return data
    except Exception as e:
        return e 
    finally:
        cursor_snowflake.close()
        conn_snowflake.close()

def insert_data_into_sqlite(data):

    conn_sqlite = sqlite3.connect(sqlite_db_file)
    cursor_sqlite = conn_sqlite.cursor()

    insert_query_sqlite = '''
    INSERT INTO insurance (policy_id, customer_name, car_model, policy_type, premium_amount)
    VALUES (?, ?, ?, ?, ?)
    '''

    try:
        cursor_sqlite.executemany(insert_query_sqlite, data)
        conn_sqlite.commit()
        print("Data inserted into SQLite successfully.")
    except sqlite3.Error as e:
        return e 
    finally:
        cursor_sqlite.close()
        conn_sqlite.close()


# Retrieve data from Snowflake
data = retrieve_data_from_snowflake()

# Insert data into SQLite
insert_data_into_sqlite(data)


#2. copying data to finance table
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

    select_query = 'SELECT * FROM finance'

    try:
        cursor_snowflake.execute(select_query)
        data = cursor_snowflake.fetchall()
        print("Data retrieved from Snowflake successfully.")
        return data
    except Exception as e:
        return e 
    finally:
        cursor_snowflake.close()
        conn_snowflake.close()


def insert_data_into_sqlite(data):

    conn_sqlite = sqlite3.connect(sqlite_db_file)
    cursor_sqlite = conn_sqlite.cursor()

    insert_query_sqlite = '''
    INSERT INTO finance (finance_id, customer_name, car_model, loan_amount, interest_rate)
    VALUES (?, ?, ?, ?, ?)
    '''

    try:
        cursor_sqlite.executemany(insert_query_sqlite, data)
        conn_sqlite.commit()
        print("Data inserted into SQLite successfully.")
    except sqlite3.Error as e:
        return e 
    finally:
        cursor_sqlite.close()
        conn_sqlite.close()


# Retrieve data from Snowflake
data = retrieve_data_from_snowflake()

# Insert data into SQLite
insert_data_into_sqlite(data)


#3. copying data to crm table
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

    select_query = 'SELECT * FROM finance'

    try:
        cursor_snowflake.execute(select_query)
        data = cursor_snowflake.fetchall()
        print("Data retrieved from Snowflake successfully.")
        return data
    except Exception as e:
        return e 
    finally:
        cursor_snowflake.close()
        conn_snowflake.close()


def insert_data_into_sqlite(data):
    conn_sqlite = sqlite3.connect(sqlite_db_file)
    cursor_sqlite = conn_sqlite.cursor()

    insert_query_sqlite = '''
    INSERT INTO Crm(customer_id, customer_name, email, phone_number, city)
    VALUES (?, ?, ?, ?, ?)
    '''

    try:
        cursor_sqlite.executemany(insert_query_sqlite, data)
        conn_sqlite.commit()
        print("Data inserted into SQLite successfully.")
    except sqlite3.Error as e:
        return e 
    finally:
        cursor_sqlite.close()
        conn_sqlite.close()


# Retrieve data from Snowflake
data = retrieve_data_from_snowflake()

# Insert data into SQLite
insert_data_into_sqlite(data)


#4. copying data to hr table
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

    select_query = 'SELECT * FROM hr'

    try:
        cursor_snowflake.execute(select_query)
        data = cursor_snowflake.fetchall()
        print("Data retrieved from Snowflake successfully.")
        return data
    except Exception as e:
        return e 
    finally:
        cursor_snowflake.close()
        conn_snowflake.close()


def insert_data_into_sqlite(data):

    conn_sqlite = sqlite3.connect(sqlite_db_file)
    cursor_sqlite = conn_sqlite.cursor()

    insert_query_sqlite = '''
    INSERT INTO hr(employee_id, employee_name, department, position, hire_date)
    VALUES (?, ?, ?, ?, ?)
    '''

    try:
        cursor_sqlite.executemany(insert_query_sqlite, data)
        conn_sqlite.commit()
        print("Data inserted into SQLite successfully.")
    except sqlite3.Error as e:
        return e 
    finally:
        cursor_sqlite.close()
        conn_sqlite.close()


# Retrieve data from Snowflake
data = retrieve_data_from_snowflake()

# Insert data into SQLite
insert_data_into_sqlite(data)




#5. copying data to it_department  table
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

    select_query = 'SELECT * FROM it_department'

    try:
        cursor_snowflake.execute(select_query)
        data = cursor_snowflake.fetchall()
        print("Data retrieved from Snowflake successfully.")
        return data
    except Exception as e:
        return e 
    finally:
        cursor_snowflake.close()
        conn_snowflake.close()


def insert_data_into_sqlite(data):

    conn_sqlite = sqlite3.connect(sqlite_db_file)
    cursor_sqlite = conn_sqlite.cursor()

    insert_query_sqlite = '''
    INSERT INTO it_department(ticket_id, issue, priority, assigned_to, status)
    VALUES (?, ?, ?, ?, ?)
    '''

    try:
        cursor_sqlite.executemany(insert_query_sqlite, data)
        conn_sqlite.commit()
        print("Data inserted into SQLite successfully.")
    except sqlite3.Error as e:
        return e 
    finally:
        cursor_sqlite.close()
        conn_sqlite.close()


# Retrieve data from Snowflake
data = retrieve_data_from_snowflake()

# Insert data into SQLite
insert_data_into_sqlite(data)


#6. copying data to sales table
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
        return data
    except Exception as e:
        return e 
    finally:
        cursor_snowflake.close()
        conn_snowflake.close()


def insert_data_into_sqlite(data):

    conn_sqlite = sqlite3.connect(sqlite_db_file)
    cursor_sqlite = conn_sqlite.cursor()

    insert_query_sqlite = '''
    INSERT INTO sales(sales_id, customer_name, car_model, purchase_date, purchase_amount)
    VALUES (?, ?, ?, ?, ?)
    '''

    try:
        cursor_sqlite.executemany(insert_query_sqlite, data)
        conn_sqlite.commit()
        print("Data inserted into SQLite successfully.")
    except sqlite3.Error as e:
        return e 
    finally:
        cursor_sqlite.close()
        conn_sqlite.close()


# Retrieve data from Snowflake
data = retrieve_data_from_snowflake()

# Insert data into SQLite
insert_data_into_sqlite(data)













