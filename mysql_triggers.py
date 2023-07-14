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

            elif table_name == "insurance":
                # Map updated data to Snowflake table columns
                policy_id = updated_data["policy_id"]
                customer_name = updated_data["customer_name"]
                car_model = updated_data["car_model"]
                policy_type = updated_data["policy_type"]
                premium_amount = updated_data["premium_amount"]
                
                # Perform UPDATE operation in Snowflake
                update_query = """
                UPDATE insurance
                SET customer_name = %s,
                    car_model = %s,
                    policy_type = %s,
                    premium_amount = %s
                WHERE policy_id = %s
                """
                snowflake_cursor.execute(update_query, (customer_name, car_model, policy_type, premium_amount, policy_id))
                snowflake_connection.commit()

            elif table_name == "finance":
                # Map updated data to Snowflake table columns
                finance_id = updated_data["finance_id"]
                customer_name = updated_data["customer_name"]
                car_model = updated_data["car_model"]
                loan_amount = updated_data["loan_amount"]
                interest_rate = updated_data["interest_rate"]
                
                # Perform UPDATE operation in Snowflake
                update_query = """
                UPDATE finance
                SET customer_name = %s,
                    car_model = %s,
                    loan_amount = %s,
                    interest_rate = %s
                WHERE finance_id = %s
                """
                snowflake_cursor.execute(update_query, (customer_name, car_model, loan_amount, interest_rate, finance_id))
                snowflake_connection.commit()

            elif table_name == "Crm":
                # Map updated data to Snowflake table columns
                customer_id = updated_data["customer_id"]
                customer_name = updated_data["customer_name"]
                email = updated_data["email"]
                phone_number = updated_data["phone_number"]
                city = updated_data["city"]
                
                # Perform UPDATE operation in Snowflake
                update_query = """
                UPDATE Crm
                SET customer_name = %s,
                    email = %s,
                    phone_number = %s,
                    city = %s
                WHERE customer_id = %s
                """
                snowflake_cursor.execute(update_query, (customer_name, email, phone_number, city, customer_id))
                snowflake_connection.commit()

            elif table_name == "hr":
                # Map updated data to Snowflake table columns
                employee_id = updated_data["employee_id"]
                employee_name = updated_data["employee_name"]
                department = updated_data["department"]
                position = updated_data["position"]
                hire_date = updated_data["hire_date"]
                
                # Perform UPDATE operation in Snowflake
                update_query = """
                UPDATE hr
                SET employee_name = %s,
                    department = %s,
                    position = %s,
                    hire_date = %s
                WHERE employee_id = %s
                """
                snowflake_cursor.execute(update_query, (employee_name, department, position, hire_date, employee_id))
                snowflake_connection.commit()

            elif table_name == "it_department":
                # Map updated data to Snowflake table columns
                ticket_id = updated_data["ticket_id"]
                issue = updated_data["issue"]
                priority = updated_data["priority"]
                assigned_to = updated_data["assigned_to"]
                status = updated_data["status"]
                
                # Perform UPDATE operation in Snowflake
                update_query = """
                UPDATE it_department
                SET issue = %s,
                    priority = %s,
                    assigned_to = %s,
                    status = %s
                WHERE ticket_id = %s
                """
                snowflake_cursor.execute(update_query, (issue, priority, assigned_to, status, ticket_id))
                snowflake_connection.commit()

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

            elif table_name == "insurance":
                # Map inserted data to Snowflake table columns
                policy_id = inserted_data["policy_id"]
                customer_name = inserted_data["customer_name"]
                car_model = inserted_data["car_model"]
                policy_type = inserted_data["policy_type"]
                premium_amount = inserted_data["premium_amount"]
                
                # Perform INSERT operation in Snowflake
                insert_query = """
                INSERT INTO insurance (policy_id, customer_name, car_model, policy_type, premium_amount)
                VALUES (%s, %s, %s, %s, %s)
                """
                snowflake_cursor.execute(insert_query, (policy_id, customer_name, car_model, policy_type, premium_amount))
                snowflake_connection.commit()

            elif table_name == "finance":
                # Map inserted data to Snowflake table columns
                finance_id = inserted_data["finance_id"]
                customer_name = inserted_data["customer_name"]
                car_model = inserted_data["car_model"]
                loan_amount = inserted_data["loan_amount"]
                interest_rate = inserted_data["interest_rate"]
                
                # Perform INSERT operation in Snowflake
                insert_query = """
                INSERT INTO finance (finance_id, customer_name, car_model, loan_amount, interest_rate)
                VALUES (%s, %s, %s, %s, %s)
                """
                snowflake_cursor.execute(insert_query, (finance_id, customer_name, car_model, loan_amount, interest_rate))
                snowflake_connection.commit()

            elif table_name == "Crm":
                # Map inserted data to Snowflake table columns
                customer_id = inserted_data["customer_id"]
                customer_name = inserted_data["customer_name"]
                email = inserted_data["email"]
                phone_number = inserted_data["phone_number"]
                city = inserted_data["city"]
                
                # Perform INSERT operation in Snowflake
                insert_query = """
                INSERT INTO Crm (customer_id, customer_name, email, phone_number, city)
                VALUES (%s, %s, %s, %s, %s)
                """
                snowflake_cursor.execute(insert_query, (customer_id, customer_name, email, phone_number, city))
                snowflake_connection.commit()

            elif table_name == "hr":
                # Map inserted data to Snowflake table columns
                employee_id = inserted_data["employee_id"]
                employee_name = inserted_data["employee_name"]
                department = inserted_data["department"]
                position = inserted_data["position"]
                hire_date = inserted_data["hire_date"]
                
                # Perform INSERT operation in Snowflake
                insert_query = """
                INSERT INTO hr (employee_id, employee_name, department, position, hire_date)
                VALUES (%s, %s, %s, %s, %s)
                """
                snowflake_cursor.execute(insert_query, (employee_id, employee_name, department, position, hire_date))
                snowflake_connection.commit()

            elif table_name == "it_department":
                # Map inserted data to Snowflake table columns
                ticket_id = inserted_data["ticket_id"]
                issue = inserted_data["issue"]
                priority = inserted_data["priority"]
                assigned_to = inserted_data["assigned_to"]
                status = inserted_data["status"]
                
                # Perform INSERT operation in Snowflake
                insert_query = """
                INSERT INTO it_department (ticket_id, issue, priority, assigned_to, status)
                VALUES (%s, %s, %s, %s, %s)
                """
                snowflake_cursor.execute(insert_query, (ticket_id, issue, priority, assigned_to, status))
                snowflake_connection.commit()

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

            elif table_name == "insurance":
                # Map deleted data to Snowflake table columns
                policy_id = deleted_data["policy_id"]
                
                # Perform DELETE operation in Snowflake
                delete_query = "DELETE FROM insurance WHERE policy_id = %s"
                snowflake_cursor.execute(delete_query, (policy_id,))
                snowflake_connection.commit()

            elif table_name == "finance":
                # Map deleted data to Snowflake table columns
                finance_id = deleted_data["finance_id"]
                
                # Perform DELETE operation in Snowflake
                delete_query = "DELETE FROM finance WHERE finance_id = %s"
                snowflake_cursor.execute(delete_query, (finance_id,))
                snowflake_connection.commit()

            elif table_name == "Crm":
                # Map deleted data to Snowflake table columns
                customer_id = deleted_data["customer_id"]
                
                # Perform DELETE operation in Snowflake
                delete_query = "DELETE FROM Crm WHERE customer_id = %s"
                snowflake_cursor.execute(delete_query, (customer_id,))
                snowflake_connection.commit()

            elif table_name == "hr":
                # Map deleted data to Snowflake table columns
                employee_id = deleted_data["employee_id"]
                
                # Perform DELETE operation in Snowflake
                delete_query = "DELETE FROM hr WHERE employee_id = %s"
                snowflake_cursor.execute(delete_query, (employee_id,))
                snowflake_connection.commit()

            elif table_name == "it_department":
                # Map deleted data to Snowflake table columns
                ticket_id = deleted_data["ticket_id"]
                
                # Perform DELETE operation in Snowflake
                delete_query = "DELETE FROM it_department WHERE ticket_id = %s"
                snowflake_cursor.execute(delete_query, (ticket_id,))
                snowflake_connection.commit()

def start_cdc_pipeline():
    global stream
    # Set up the binlog stream reader
   
    stream = BinLogStreamReader(
        connection_settings=mysql_config,
        server_id=1,  # Unique ID for the MySQL slave
        blocking=True,
        resume_stream=True,
        only_events=[UpdateRowsEvent, WriteRowsEvent, DeleteRowsEvent],
        only_schemas=[mysql_config['database']],
        only_tables=['sales', 'insurance', 'finance', 'Crm', 'hr', 'it_department'],)
    
    for binlogevent in stream:
        for row in binlogevent.rows:
            event = {"table": binlogevent.table, "operation": binlogevent.__class__.__name__}
            if isinstance(binlogevent, UpdateRowsEvent):
                event["data"] = row["after_values"]
            elif isinstance(binlogevent, WriteRowsEvent):
                event["data"] = row["values"]
            elif isinstance(binlogevent, DeleteRowsEvent):
                event["data"] = row["values"]
            process_event(event)

    # Close the binlog stream
    stream.close()

# Start the CDC pipeline
start_cdc_pipeline()

