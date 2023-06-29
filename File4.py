# import sqlite3
# connection = sqlite3.connect('Automobile.db')
# cursor = connection.cursor()

# #creating the cdc tables in the database

# cursor.execute('''
#     CREATE TABLE cdc_inserts3(
#     id int primary key,
#     example2 TEXT,
#     name2 text,
#     age2 text,
#     timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
#     )
# ''')

# cursor.execute('''
#     CREATE TABLE cdc_updates3(
#     id int primary key,
#     example2 TEXT,
#     name2 text,
#     age2 text,
#     timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
#     )
# ''')
               
# cursor.execute('''
#     CREATE TABLE cdc_deletes3(
#     id int primary key,
#     example2 TEXT,
#     name2 text,
#     age2 text,
#     timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
#     )
# ''')
               
# connection.commit()


# #defining triggers to capture changes in the source tables
# def create_insert_trigger(table_name):
#     cursor.execute('''
#         CREATE TRIGGER IF NOT EXISTS cdc_insert_trigger_{}
#         AFTER INSERT ON {}
#         FOR EACH ROW
#         BEGIN
#             INSERT INTO cdc_inserts3(table_name, name2, age2) VALUES ('{}', NEW.name2, NEW.age2);
#         END
#     '''.format(table_name, table_name, table_name))

# create_insert_trigger('example2')
# print("insert trigger created")

# def create_update_trigger(table_name):
#     cursor.execute('''
#         CREATE TRIGGER IF NOT EXISTS cdc_update_trigger_{}
#         AFTER UPDATE ON {}
#         FOR EACH ROW
#         BEGIN
#             INSERT INTO cdc_updates(table_name, name, cage) VALUES ('{}', NEW.name, NEW.age);
#         END
#     '''.format(table_name,table_name,table_name))

# create_update_trigger('example')

# def create_delete_trigger(table_name):
#     cursor.execute('''
#         CREATE TRIGGER IF NOT EXISTS cdc_delete_trigger_{}
#         AFTER DELETE ON {}
#         FOR EACH ROW
#         BEGIN
#             INSERT INTO cdc_deletes(table_name, name, age) VALUES ('{}', OLD.name, OLD.age);
#         END
#     '''.format(table_name,table_name, table_name))

# create_delete_trigger('example')

# connection.commit()

# #testing the cdc 
# #insert in example table
# cursor.execute("INSERT INTO example (name, age) VALUES ('veda', '20')")
# connection.commit()


# # Query CDC inserts
# cursor.execute("SELECT * FROM cdc_inserts")
# inserts = cursor.fetchall()
# for row in inserts:
#     print(row)

# connection.close()