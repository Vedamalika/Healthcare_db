import psycopg2

def create_postgresql_connection():
    try:
        conn_postgresql = psycopg2.connect(
            host="localhost",
            port="5432",
            user="postgres",
            password="Kunjara123@",
        )
        print("PostgreSQL connection created")
        return conn_postgresql

    except psycopg2.Error as e:
        print("PostgreSQL connection error:", e)


# Creating the PostgreSQL connection
conn_postgresql = create_postgresql_connection()

# Creating a database
def create_database(conn, database_name):
    try:
        cursor_postgresql = conn.cursor()

        create_database_query = f"CREATE DATABASE {database_name}"
        cursor_postgresql.execute(create_database_query)
        print(f"Database '{database_name}' is created")

    except psycopg2.Error as e:
        return e

    except Exception as e:
        return e

    cursor_postgresql.close()
    conn.close()


database_name = "Automobile"
# Calling the function to create the database
create_database(conn_postgresql, database_name)


def create_tables_postgresql(conn):
    try:
        cursor_postgresql = conn.cursor()

        cursor_postgresql.execute("SET search_path TO Automobile")

        create_table1_query = """
        CREATE TABLE sales (
            sales_id SERIAL PRIMARY KEY,
            customer_name VARCHAR(255),
            car_model VARCHAR(255),
            purchase_date DATE,
            purchase_amount FLOAT
        )
        """
        cursor_postgresql.execute(create_table1_query)
        print("Table 'sales' is created successfully")

        create_table2_query = """
        CREATE TABLE insurance (
            policy_id SERIAL PRIMARY KEY,
            customer_name VARCHAR(255),
            car_model VARCHAR(255),
            policy_type VARCHAR(255),
            premium_amount FLOAT
        )
        """
        cursor_postgresql.execute(create_table2_query)
        print("Table 'insurance' is created successfully")

        create_table3_query = """
        CREATE TABLE finance (
            finance_id SERIAL PRIMARY KEY,
            customer_name VARCHAR(255),
            car_model VARCHAR(255),
            loan_amount FLOAT,
            interest_rate FLOAT
        )
        """
        cursor_postgresql.execute(create_table3_query)
        print("Table 'finance' is created successfully")

        create_table4_query = """
        CREATE TABLE Crm (
            customer_id SERIAL PRIMARY KEY,
            customer_name VARCHAR(255),
            email VARCHAR(255),
            phone_number VARCHAR(255),
            city VARCHAR(255)
        )
        """
        cursor_postgresql.execute(create_table4_query)
        print("Table 'Crm' is created successfully")

        create_table5_query = """
        CREATE TABLE hr (
            employee_id SERIAL PRIMARY KEY,
            employee_name VARCHAR(255),
            department VARCHAR(255),
            position VARCHAR(255),
            hire_date DATE
        )
        """
        cursor_postgresql.execute(create_table5_query)
        print("Table 'hr' is created successfully")

        create_table6_query = """
        CREATE TABLE it_department (
            ticket_id SERIAL PRIMARY KEY,
            issue VARCHAR(255),
            priority VARCHAR(255),
            assigned_to VARCHAR(255),
            status VARCHAR(255)
        )
        """
        cursor_postgresql.execute(create_table6_query)
        print("Table 'it_department' is created successfully")

        conn.commit()

    except psycopg2.Error as e:
        print(f"An error occurred: {e}")
    except Exception as e:
        return e

    cursor_postgresql.close()
    conn.close()

# Calling the function for creating the tables in PostgreSQL
create_tables_postgresql(conn_postgresql)



