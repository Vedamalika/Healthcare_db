import mysql.connector

def create_mysql_connection():
    try:
        conn_mysql = mysql.connector.connect(
            host="localhost",
            user="root",
            password="Kunjara123@"
        )
        print("MySQL connection created")
        return conn_mysql

    except mysql.connector.Error as e:
        print("MySQL connection error:", e)



# Creating the MySQL connection
conn_mysql = create_mysql_connection()

#creating a database
def create_database(conn, database_name):
    try:
        cursor_mysql = conn.cursor()

        create_database_query = f"CREATE DATABASE {database_name}"
        cursor_mysql.execute(create_database_query)
        print(f"Database '{database_name}' is created")

    except mysql.connector.Error as e:
        return e

    except Exception as e:
        return e

    cursor_mysql.close()
    conn.close()

database_name = "Automobile"
# Calling the function to create the database

# create_database(conn_mysql, database_name)


def create_tables_mysql(conn):
    try:
        cursor_mysql = conn.cursor()

        cursor_mysql.execute("USE Automobile")

        create_table1_query = """
        CREATE TABLE sales (
            sales_id INT,
            customer_name VARCHAR(255),
            car_model VARCHAR(255),
            purchase_date DATE,
            purchase_amount FLOAT
        )
        """
        cursor_mysql.execute(create_table1_query)
        print("Table 'sales' is created successfully")

        create_table2_query = """
        CREATE TABLE insurance (
            policy_id INT,
            customer_name VARCHAR(255),
            car_model VARCHAR(255),
            policy_type VARCHAR(255),
            premium_amount FLOAT
        )
        """
        cursor_mysql.execute(create_table2_query)
        print("Table 'insurance' is created successfully")

        create_table3_query = """
        CREATE TABLE finance (
            finance_id INT,
            customer_name VARCHAR(255),
            car_model VARCHAR(255),
            loan_amount FLOAT,
            interest_rate FLOAT
        )
        """
        cursor_mysql.execute(create_table3_query)
        print("Table 'finance' is created successfully")

        create_table4_query = """
        CREATE TABLE Crm (
            customer_id INT,
            customer_name VARCHAR(255),
            email VARCHAR(255),
            phone_number VARCHAR(255),
            city VARCHAR(255)
        )
        """
        cursor_mysql.execute(create_table4_query)
        print("Table 'Crm' is created successfully")

        create_table5_query = """
        CREATE TABLE hr (
            employee_id INT,
            employee_name VARCHAR(255),
            department VARCHAR(255),
            position VARCHAR(255),
            hire_date DATE
        )
        """
        cursor_mysql.execute(create_table5_query)
        print("Table 'hr' is created successfully")

        create_table6_query = """
        CREATE TABLE it_department (
            ticket_id INT,
            issue VARCHAR(255),
            priority VARCHAR(255),
            assigned_to VARCHAR(255),
            status VARCHAR(255)
        )
        """
        cursor_mysql.execute(create_table6_query)
        print("Table 'it_department' is created successfully")

        conn.commit()

    except mysql.connector.Error as e:
        print(f"An error occurred: {e}")
    except Exception as e:
        return e

    cursor_mysql.close()

    conn.close()

# Calling the function for creating the tables in MySQL
create_tables_mysql(conn_mysql)


#inserting data into the tables





