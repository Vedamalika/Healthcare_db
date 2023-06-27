import snowflake.connector
from faker import Faker

    
snowflake_config = {
    'user': 'VEDAMALIKA',
    'password': '9@Vedasai',
    'account': 'qf67219.ap-southeast-1',
    'warehouse': 'HEALTHCARE_DB_WH',
    'database': 'Healthcare',
    'schema': 'PUBLIC'
}
    
try:
    snowflake_conn = snowflake.connector.connect(**snowflake_config)
    snowflake_cursor = snowflake_conn.cursor()
    print("Snowflake connection created successfully")
    
except snowflake.connector.Error as e:
    print("Snowflake Error: {0}".format(e))
    
except Exception as e:
    print("Error: {0}".format(e))
        

def create_tables(snowflake_cursor):
    try:
        create_table1_query = """
        CREATE OR REPLACE TABLE sales (
            sales_id INT,
            customer_name STRING,
            car_model STRING,
            purchase_date DATE,
            purchase_amount FLOAT
        )
        """
        snowflake_cursor.execute(create_table1_query)
        print("Table 'sales' is created successfully")

        create_table2_query = """
        CREATE OR REPLACE TABLE insurance (
            policy_id INT,
            customer_name STRING,
            car_model STRING,
            policy_type STRING,
            premium_amount FLOAT
        )
        """
        snowflake_cursor.execute(create_table2_query)
        print("Table 'insurance' is created successfully")

        create_table3_query = """
        CREATE OR REPLACE TABLE finance (
            finance_id INT,
            customer_name STRING,
            car_model STRING,
            loan_amount FLOAT,
            interest_rate FLOAT
        )
        """
        snowflake_cursor.execute(create_table3_query)
        print("Table 'finance' is created successfully")

        create_table4_query = """
        CREATE OR REPLACE TABLE Crm (
            customer_id INT,
            customer_name STRING,
            email STRING,
            phone_number STRING,
            city STRING
        )
        """
        snowflake_cursor.execute(create_table4_query)
        print("Table 'Crm' is created successfully")

        create_table5_query = """
        CREATE OR REPLACE TABLE hr (
            employee_id INT,
            employee_name STRING,
            department STRING,
            position STRING,
            hire_date DATE
        )
        """
        snowflake_cursor.execute(create_table5_query)
        print("Table 'hr' is created successfully")

        create_table6_query = """
        CREATE OR REPLACE TABLE it_department (
            ticket_id INT,
            issue STRING,
            priority STRING,
            assigned_to STRING,
            status STRING
        )
        """
        snowflake_cursor.execute(create_table6_query)
        print("Table 'it_department' is created successfully")

    
    except snowflake.connector.Error as e:
        print("Snowflake Error: {0}".format(e))

    except Exception as e:
        print("Error: {0}".format(e))

create_tables(snowflake_cursor)







snowflake_cursor.close()
snowflake_conn.close()


