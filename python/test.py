import psycopg2
import sys
# Currently hard-coded, only works on team account because of DB_USER
DB_HOST = 'xcne0'
DB_PORT = '5115'
DB_NAME = 'project'
DB_USER = 'cs4224s'

def load_test_data():
    try:
        connection = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
        )
        cursor = connection.cursor()

        # 1. Create a Distributed Table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id serial PRIMARY KEY,
                name text,
                email text
            );
        """)
        cursor.execute("SELECT create_distributed_table('users', 'id');")

        # 2. Insert Data into the Table
        cursor.execute("""
            INSERT INTO users (name, email) VALUES
            ('Alice', 'alice@example.com'),
            ('Bob', 'bob@example.com'),
            ('Charlie', 'charlie@example.com');
        """)

        # Commit the changes
        connection.commit()
    except Exception as e:
        print(f"An error occurred: {e}")

    finally:
        # Close the cursor and connection
        if cursor:
            cursor.close()
        if connection:
            connection.close()

def run_test_query():
    connection = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
        )
    cursor = connection.cursor()

    cursor.execute("SELECT * FROM users;")
    users = cursor.fetchall()
    print("Users in the table:")
    for user in users:
        print(user)

    cursor.execute("SELECT * FROM citus_get_active_worker_nodes();")
    worker_nodes = cursor.fetchall()
    print("Active Worker Nodes:")
    for node in worker_nodes:
        print(node)

    cursor.execute("SELECT COUNT(*) FROM users;")
    count = cursor.fetchone()[0]
    print(f"Total number of users: {count}")

def check_test_partitions():
    connection = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
        )
    cur = connection.cursor()

    cur.execute("""
        SELECT partition_name
        FROM citus_get_table_partitions('public.users');
    """)
    partitions = cur.fetchall()

    for partition in partitions:
        partition_name = partition[0]
        print(f"Records from partition: {partition_name}")
        
        cur.execute(f"SELECT * FROM {partition_name};")
        records = cur.fetchall()
        
        for record in records:
            print(record)

    # Close the connection
    cur.close()
    connection.close()

def main():
    if sys.argv[1] == 'load':
        load_test_data()
    elif sys.argv[1] == 'worker':
        check_test_partitions()
    else:
        run_test_query()
    

if __name__ == "__main__":
    main()