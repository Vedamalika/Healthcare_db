import sqlite3

def create_sqlite_connection(database_name):
    try:
        conn = sqlite3.connect(database_name)
        print("SQLite connection created")
        return conn
    
    except sqlite3.Error as e:
        return e

# Create SQLite connection
database_name = 'Healthcare.db'
conn_sqlite = create_sqlite_connection(database_name)

