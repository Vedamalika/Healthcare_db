import sqlite3
connection = sqlite3.connect('Automobile.db')
cursor = connection.cursor()

# creating a cdc tables in the database

# cursor.execute('''
#     CREATE TABLE cdc_inserts_sales (
#     id INT primary key,
#     sales Text,
#     sales_id INT,
#     customer_name STRING,
#     car_model STRING,
#     purchase_date DATE,
#     purchase_amount FLOAT,
#     timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
#     )
# ''')
               
# cursor.execute('''
#     CREATE TABLE cdc_updates_sales (
#     id INT primary key,
#     sales Text,
#     sales_id INT,
#     customer_name STRING,
#     car_model STRING,
#     purchase_date DATE,
#     purchase_amount FLOAT,
#     timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
#     )
# ''')
               
# cursor.execute('''
#     CREATE TABLE cdc_deletes_sales (
#     id INT primary key,
#     sales Text,
#     sales_id INT,
#     customer_name STRING,
#     car_model STRING,
#     purchase_date DATE,
#     purchase_amount FLOAT,
#     timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
#     )
# ''')
               
# cursor.execute('''
#     CREATE TABLE cdc_inserts_insurance (
#     id INT primary key,
#     insurance Text,
#     policy_id INT,
#     customer_name STRING,
#     car_model STRING,
#     policy_type STRING,
#     premium_amount FLOAT
#     timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
#     )
# ''')
# cursor.execute('''
#     CREATE TABLE cdc_updates_insurance (
#     id INT primary key,
#     insurance Text,
#     policy_id INT,
#     customer_name STRING,
#     car_model STRING,
#     policy_type STRING,
#     premium_amount FLOAT
#     timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
#     )
# ''')

# cursor.execute('''
#     CREATE TABLE cdc_deletes_insurance (
#     id INT primary key,
#     insurance Text,
#     policy_id INT,
#     customer_name STRING,
#     car_model STRING,
#     policy_type STRING,
#     premium_amount FLOAT
#     timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
#     )
# ''')

# def create_insert_trigger(table_name):
#     cursor.execute('''
#         CREATE TRIGGER IF NOT EXISTS cdc_insert_trigger_{}
#         AFTER INSERT ON {}
#         FOR EACH ROW
#         BEGIN
#             INSERT INTO cdc_inserts_sales (sales,sales_id,customer_name,car_model,purchase_date,purchase_amount) VALUES ('{}', NEW.sales_id, NEW.customer_name, NEW.car_model, NEW.purchase_date, NEW.purchase_amount);
#         END
#     '''.format(table_name, table_name, table_name))


# create_insert_trigger('sales')

# def create_update_trigger(table_name):
#     cursor.execute('''
#         CREATE TRIGGER IF NOT EXISTS cdc_update_trigger_{}
#         AFTER UPDATE ON {}
#         FOR EACH ROW
#         BEGIN
#             INSERT INTO cdc_updates_sales (sales,sales_id,customer_name,car_model,purchase_date,purchase_amount) VALUES ('{}', NEW.sales_id, NEW.customer_name, NEW.car_model, NEW.purchase_date, NEW.purchase_amount);
#         END
#     '''.format(table_name, table_name, table_name))

# create_update_trigger('sales')

# def create_delete_trigger(table_name):
#     cursor.execute('''
#         CREATE TRIGGER IF NOT EXISTS cdc_delete_trigger_{}
#         AFTER DELETE ON {}
#         FOR EACH ROW
#         BEGIN
#             INSERT INTO cdc_deletes_sales (sales,sales_id,customer_name,car_model,purchase_date,purchase_amount) VALUES ('{}', OLD.sales_id, OLD.customer_name, OLD.car_model, OLD.purchase_date, OLD.purchase_amount);
#         END
#     '''.format(table_name, table_name, table_name))

# create_delete_trigger('sales')

# connection.commit()

# Testing the CDC - Inserting into the 'SALES' table
# cursor.execute("INSERT INTO sales (sales_id,customer_name,car_model,purchase_date,purchase_amount) VALUES (12000,'veda','tax',2008-03-10,32.65)")
# connection.commit()

# cursor.execute("SELECT * FROM cdc_inserts_sales")
# inserts = cursor.fetchall()
# for row in inserts:
#     print(row)

connection.close()