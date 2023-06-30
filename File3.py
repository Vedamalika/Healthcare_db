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




