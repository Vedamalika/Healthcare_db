import snowflake.connector
import mysql.connector


def create_mysql_connection():
    try:
        conn_mysql = mysql.connector.connect(
            host='localhost',
            user='root',
            password='Kunjara123@',
            database='Automobile'
        )
        print("MySQL connection created")
        return conn_mysql

    except mysql.connector.Error as e:
        return e


# Snowflake connection parameters
snowflake_account = 'qf67219.ap-southeast-1'
snowflake_user = 'VEDAMALIKA'
snowflake_password = '9@Vedasai'
snowflake_warehouse = 'Automobile_db'
snowflake_database = 'Automobile'
snowflake_schema = 'PUBLIC'

# MySQL connection parameters
mysql_host = 'localhost'
mysql_user = 'root'
mysql_password = 'Kunjara123@'
mysql_database = 'Automobile'

#1. Copying data to the insurance table
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


def insert_data_into_mysql(data):
    conn_mysql = mysql.connector.connect(
        host=mysql_host,
        user=mysql_user,
        password=mysql_password,
        database=mysql_database
    )
    cursor_mysql = conn_mysql.cursor()

    insert_query_mysql = '''
    INSERT INTO insurance (policy_id, customer_name, car_model, policy_type, premium_amount)
    VALUES (%s, %s, %s, %s, %s)
    '''

    try:
        cursor_mysql.executemany(insert_query_mysql, data)
        conn_mysql.commit()
        print("Data inserted into MySQL successfully.")
    except mysql.connector.Error as e:
        return e 
    finally:
        cursor_mysql.close()
        conn_mysql.close()


# Retrieve data from Snowflake
data = retrieve_data_from_snowflake()

# Insert data into MySQL
insert_data_into_mysql(data)


#2. Copying data to the finance table
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


def insert_data_into_mysql(data):
    conn_mysql = mysql.connector.connect(
        host=mysql_host,
        user=mysql_user,
        password=mysql_password,
        database=mysql_database
    )
    cursor_mysql = conn_mysql.cursor()

    insert_query_mysql = '''
    INSERT INTO finance (finance_id, customer_name, car_model, loan_amount, interest_rate)
    VALUES (%s, %s, %s, %s, %s)
    '''

    try:
        cursor_mysql.executemany(insert_query_mysql, data)
        conn_mysql.commit()
        print("Data inserted into MySQL successfully.")
    except mysql.connector.Error as e:
        return e 
    finally:
        cursor_mysql.close()
        conn_mysql.close()


# Retrieve data from Snowflake
data = retrieve_data_from_snowflake()

# Insert data into MySQL
insert_data_into_mysql(data)


#3. Copying data to the crm table
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

    select_query = 'SELECT * FROM crm'

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


def insert_data_into_mysql(data):
    conn_mysql = mysql.connector.connect(
        host=mysql_host,
        user=mysql_user,
        password=mysql_password,
        database=mysql_database
    )
    cursor_mysql = conn_mysql.cursor()

    insert_query_mysql = '''
    INSERT INTO Crm (customer_id, customer_name, email, phone_number, city)
    VALUES (%s, %s, %s, %s, %s)
    '''

    try:
        cursor_mysql.executemany(insert_query_mysql, data)
        conn_mysql.commit()
        print("Data inserted into MySQL successfully.")
    except mysql.connector.Error as e:
        return e 
    finally:
        cursor_mysql.close()
        conn_mysql.close()


# Retrieve data from Snowflake
data = retrieve_data_from_snowflake()

# Insert data into MySQL
insert_data_into_mysql(data)


#4. Copying data to the hr table
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


def insert_data_into_mysql(data):
    conn_mysql = mysql.connector.connect(
        host=mysql_host,
        user=mysql_user,
        password=mysql_password,
        database=mysql_database
    )
    cursor_mysql = conn_mysql.cursor()

    insert_query_mysql = '''
    INSERT INTO hr (employee_id, employee_name, department, position, hire_date)
    VALUES (%s, %s, %s, %s, %s)
    '''

    try:
        cursor_mysql.executemany(insert_query_mysql, data)
        conn_mysql.commit()
        print("Data inserted into MySQL successfully.")
    except mysql.connector.Error as e:
        return e 
    finally:
        cursor_mysql.close()
        conn_mysql.close()


# Retrieve data from Snowflake
data = retrieve_data_from_snowflake()

# Insert data into MySQL
insert_data_into_mysql(data)


#5. Copying data to the it_department table
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


def insert_data_into_mysql(data):
    conn_mysql = mysql.connector.connect(
        host=mysql_host,
        user=mysql_user,
        password=mysql_password,
        database=mysql_database
    )
    cursor_mysql = conn_mysql.cursor()

    insert_query_mysql = '''
    INSERT INTO it_department (ticket_id, issue, priority, assigned_to, status)
    VALUES (%s, %s, %s, %s, %s)
    '''

    try:
        cursor_mysql.executemany(insert_query_mysql, data)
        conn_mysql.commit()
        print("Data inserted into MySQL successfully.")
    except mysql.connector.Error as e:
        return e 
    finally:
        cursor_mysql.close()
        conn_mysql.close()


# Retrieve data from Snowflake
data = retrieve_data_from_snowflake()

# Insert data into MySQL
insert_data_into_mysql(data)


#6. Copying data to the sales table
def retrieve_data_from_snowflake():
    conn_snowflake = snowflake.connector.connect(
        user=snowflake_user,
        password=snowflake_password,
        account=snowflake_account,
        warehouse=snowflake_warehouse,def retrieve_data_from_snowflake():
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


def insert_data_into_mysql(data):
    conn_mysql = mysql.connector.connect(
        host=mysql_host,
        user=mysql_user,
        password=mysql_password,
        database=mysql_database
    )
    cursor_mysql = conn_mysql.cursor()

    insert_query_mysql = '''
    INSERT INTO sales (sales_id, customer_name, car_model, purchase_date, purchase_amount)
    VALUES (%s, %s, %s, %s, %s)
    '''

    try:
        cursor_mysql.executemany(insert_query_mysql, data)
        conn_mysql.commit()
        print("Data inserted into MySQL successfully.")
    except mysql.connector.Error as e:
        return e 
    finally:
        cursor_mysql.close()
        conn_mysql.close()


# Retrieve data from Snowflake
data = retrieve_data_from_snowflake()

# Insert data into MySQL
insert_data_into_mysql(data)

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


def insert_data_into_mysql(data):
    conn_mysql = mysql.connector.connect(
        host=mysql_host,
        user=mysql_user,
        password=mysql_password,
        database=mysql_database
    )
    cursor_mysql = conn_mysql.cursor()

    insert_query_mysql = '''
    INSERT INTO sales (sales_id, customer_name, car_model, purchase_date, purchase_amount)
    VALUES (%s, %s, %s, %s, %s)
    '''

    try:
        cursor_mysql.executemany(insert_query_mysql, data)
        conn_mysql.commit()
        print("Data inserted into MySQL successfully.")
    except mysql.connector.Error as e:
        return e 
    finally:
        cursor_mysql.close()
        conn_mysql.close()


# Retrieve data from Snowflake
data = retrieve_data_from_snowflake()

# Insert data into MySQL
insert_data_into_mysql(data)

