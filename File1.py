import snowflake.connector
from faker import Faker
import random


#snowflake connection parameters   
snowflake_config = {
    'user': 'VEDAMALIKA',
    'password': '9@Vedasai',
    'account': 'qf67219.ap-southeast-1',
    'warehouse': 'Automobile_db',
    'database': 'Automobile',
    'schema': 'PUBLIC'
}

#creating a snowflake connection  and cursor object
try:
    snowflake_conn = snowflake.connector.connect(**snowflake_config)
    snowflake_cursor = snowflake_conn.cursor()
    print("Snowflake connection created successfully")
    
except snowflake.connector.Error as e:
    print("Snowflake Error: {0}".format(e))
    
except Exception as e:
    print("Error: {0}".format(e))
        
#creating tables in snowflake warehouse
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
        #executing the create table query 
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

# create_tables(snowflake_cursor)


#inserting data into sales table
def insert_data_into_sales(snowflake_conn, snowflake_cursor):
    fake = Faker()

    insert_query = '''
    INSERT INTO sales (sales_id, customer_name, car_model, purchase_date, purchase_amount)
    VALUES (%s, %s, %s, %s, %s)
    '''

    for i in range(1, 101):
        sales_id = i
        customer_name = fake.name()
        car_model = fake.word()
        purchase_date = fake.date()
        purchase_amount = round(random.uniform(10, 100), 2)

        try:
            snowflake_cursor.execute(insert_query, (sales_id, customer_name, car_model, purchase_date, purchase_amount))
        
        except snowflake.connector.Error as e:
            print("Snowflake Error: {0}".format(e))
        except Exception as e:
            return e
        
    
    snowflake_conn.commit()

# insert_data_into_sales(snowflake_conn, snowflake_cursor)
# print("Data inserted into sales table")


#inserting data into insurance table
def insert_data_into_insurance(snowflake_conn,snowflake_cursor):
    fake = Faker()

    insert_query1 = '''
    INSERT INTO insurance (policy_id, customer_name, car_model, policy_type, premium_amount) 
    VALUES (%s, %s, %s, %s, %s)
    '''

    for i in range(1,101):
        policy_id = i
        customer_name = fake.name()
        car_model = fake.word()
        policy_type = fake.word()
        premium_amount = round(random.uniform(100,100),2)

        try:
            snowflake_cursor.execute(insert_query1,(policy_id, customer_name,car_model,policy_type,premium_amount))
        
        except snowflake.connector.Error as e:
            print("Snowflake Error: {0}".format(e))
        except Exception as e:
            return e 
    
    snowflake_conn.commit()

# insert_data_into_insurance(snowflake_conn, snowflake_cursor)
# print("Data inserted into insurance table")


#inserting data into finance table
def insert_data_into_finance(snowflake_conn,snowflake_cursor):
    fake = Faker()

    insert_query2 = '''
    INSERT INTO finance (finance_id, customer_name, car_model, loan_amount, interest_rate)
    VALUES (%s, %s, %s, %s, %s)
    '''

    for i in range(1, 101):
        finance_id = i
        customer_name = fake.name()
        car_model = fake.word()
        loan_amount = round(random.uniform(5000, 50000), 2)
        interest_rate = round(random.uniform(2, 10), 2)

        try:
            snowflake_cursor.execute(insert_query2, (finance_id, customer_name, car_model, loan_amount, interest_rate))
        
        except snowflake.connector.Error as e:
            print("Snowflake Error: {0}".format(e))
        except Exception as e:
            return e 
    
    snowflake_conn.commit()

# insert_data_into_finance(snowflake_conn, snowflake_cursor)
# print("Data inserted into finance table")


#inserting data into crm table
def insert_data_into_crm(snowflake_conn,snowflake_cursor):
    fake = Faker()

    insert_query3 = '''
    INSERT INTO Crm (customer_id, customer_name, email, phone_number, city)
    VALUES (%s, %s, %s, %s, %s)
    '''

    for i in range(1, 101):
        customer_id = i
        customer_name = fake.name()
        email = fake.email()
        phone_number = fake.phone_number()
        city = fake.city()

        try:
            snowflake_cursor.execute(insert_query3, (customer_id, customer_name, email, phone_number, city))
        
        except snowflake.connector.Error as e:
            print("Snowflake Error: {0}".format(e))
        except Exception as e:
            return e 
    
    snowflake_conn.commit()

# insert_data_into_crm(snowflake_conn, snowflake_cursor)
# print("Data inserted into crm table")



#inserting data into hr table
def insert_data_into_hr(snowflake_conn,snowflake_cursor):
    fake = Faker()

    insert_query4 = '''
    INSERT INTO hr (employee_id, employee_name, department, position, hire_date)
    VALUES (%s, %s, %s, %s, %s)
    '''
    job =['sales','crm','hr','it','front office']
    job_title = ['admin', 'java developer', 'python developer' ,'data scientist','data engineer','dot net developer','software developer','asociate software developer','process associate','Team lead','project manager','associate']
    for i in range(1, 101):
        employee_id = i
        employee_name = fake.name()
        department = random.choice(job)
        position = random.choice(job_title)
        hire_date = fake.date()

        try:
            snowflake_cursor.execute(insert_query4, (employee_id, employee_name, department, position, hire_date))
        
        except snowflake.connector.Error as e:
            print("Snowflake Error: {0}".format(e))
        except Exception as e:
            return e 
    
    snowflake_conn.commit()

# insert_data_into_hr(snowflake_conn, snowflake_cursor)
# print("Data inserted into hr table")


#inserting data into It table
def insert_data_into_It(snowflake_conn,snowflake_cursor):
    fake = Faker()

    insert_query5 = '''
    INSERT INTO it_department (ticket_id, issue, priority, assigned_to, status)
    VALUES (%s, %s, %s, %s, %s)
    '''

    priorities = ['Low', 'Medium', 'High']
    status = ['Open', 'In Progress', 'Closed']

    for i in range(1, 101):
        ticket_id = i
        issue = fake.sentence(nb_words=6)
        priority = random.choice(priorities)
        assigned_to = fake.name()
        status = random.choice(status)

        try:
            snowflake_cursor.execute(insert_query5, (ticket_id, issue, priority, assigned_to, status))
        
        except snowflake.connector.Error as e:
            print("Snowflake Error: {0}".format(e))
        except Exception as e:
            return e 
    
    snowflake_conn.commit()

# insert_data_into_It(snowflake_conn, snowflake_cursor)
# print("Data inserted into It table")


# # creating the stream objects for each table to track the dml operations
# def create_stream(stream_table, table_name):
#         try:
#         # Create the stream
#             create_stream_query = f"CREATE STREAM {stream_table} ON TABLE {table_name}"
#             snowflake_cursor.execute(create_stream_query)
#             print(f"Stream {stream_table} created for table {table_name}")

#         except snowflake.connector.Error as e:
#             print("Snowflake Error: {0}".format(e))
#         except Exception as e:
#             return e 
            

# create_stream("sales_stream", "sales")
# create_stream("insurance_stream", "insurance")
# create_stream("finance_stream","finance")
# create_stream("crm_stream",'Crm')
# create_stream("hr_stream","hr")
# create_stream("It_stream","it_department")


 # Query the streams
snowflake_cursor.execute("SELECT * FROM finance_stream")
result1 = snowflake_cursor.fetchall()
for row in result1:
    print(row)

snowflake_cursor.execute("SELECT * FROM insurance_stream")
result2 = snowflake_cursor.fetchall()
for row in result2:
    print(row)





#closing the cursor and connection
snowflake_cursor.close()
snowflake_conn.close()




