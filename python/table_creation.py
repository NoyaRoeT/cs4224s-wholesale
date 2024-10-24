import psycopg2
import os

DB_HOST = 'localhost'
DB_PORT = os.getenv('PGPORT', '5115')
DB_NAME = os.getenv('PGDATABASE', 'project')
DB_USER = os.getenv('PGUSER', 'cs4224s')

def create_tables():
    connection = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
    )
    cursor = connection.cursor()
    try:

        # 1. Create warehouse table
        print(f"warehouse table creation starts")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS warehouse (
                w_id INT PRIMARY KEY,
                w_name VARCHAR(10),
                w_street_1 VARCHAR(20),
                w_street_2 VARCHAR(20),
                w_city VARCHAR(20),
                w_state CHAR(2),
                w_zip CHAR(9),
                w_tax DECIMAL(4,4),
                w_ytd DECIMAL(12,2)
            );
        """)
        print(f"warehouse table creation ends")
        print(f"warehouse table distribution starts")
        cursor.execute(create_partition_key("warehouse","w_id"))
        # cursor.execute(check_partition_key("warehouse"))
        # result = cursor.fetchone()
        # print(result[0] +" partition key: "+ result[1])
        print(f"warehouse table distribution ends")

        # 2. Create district table
        print(f"district table creation starts")
        cursor.execute("""
           CREATE TABLE IF NOT EXISTS district (
                d_w_id INT,
                d_id INT,
                d_name VARCHAR(10),
                d_street_1 VARCHAR(20),
                d_street_2 VARCHAR(20),
                d_city VARCHAR(20),
                d_state CHAR(2),
                d_zip CHAR(9),
                d_tax DECIMAL(4,4),
                d_ytd DECIMAL(12,2),
                d_next_o_id INT,
                PRIMARY KEY (d_w_id, d_id),
                FOREIGN KEY (d_w_id) REFERENCES warehouse (w_id)
            );
        """)
        print(f"district table creation ends")
        print(f"district table distribution starts")
        cursor.execute(create_partition_key("district","d_w_id"))
        # cursor.execute(check_partition_key("district"))
        # result = cursor.fetchone()
        # print(result[0] +" partition key: "+ result[1])
        print(f"district table distribution ends")

        # 3. Create customer table
        print(f"customer table creation starts")
        cursor.execute("""
           CREATE TABLE IF NOT EXISTS customer (
                c_w_id INT,
                c_d_id INT,
                c_id INT,
                c_first VARCHAR(16),
                c_middle CHAR(2),
                c_last VARCHAR(16),
                c_street_1 VARCHAR(20),
                c_street_2 VARCHAR(20),
                c_city VARCHAR(20),
                c_state CHAR(2),
                c_zip CHAR(9),
                c_phone CHAR(16),
                c_since TIMESTAMP,
                c_credit CHAR(2),
                c_credit_lim DECIMAL(12,2),
                c_discount DECIMAL(5,4),
                c_balance DECIMAL(12,2),
                c_ytd_payment FLOAT,
                c_payment_cnt INT,
                c_delivery_cnt INT,
                c_data VARCHAR(500),
                PRIMARY KEY (c_w_id, c_d_id, c_id),    -- Composite primary key
                FOREIGN KEY (c_w_id, c_d_id) REFERENCES district (d_w_id, d_id)  -- Foreign key to district table
            );
        """)
        print(f"customer table creation ends")
        print(f"customer table distribution starts")
        cursor.execute(create_partition_key("customer","c_w_id"))
        # cursor.execute(check_partition_key("customer"))
        # result = cursor.fetchone()
        # print(result[0] +" partition key: "+ result[1])
        print(f"customer table distribution ends")

        # 4. Create order table
        print(f"order table creation starts")
        cursor.execute("""
           CREATE TABLE IF NOT EXISTS "order" (
                o_w_id INT,
                o_d_id INT,
                o_id INT,
                o_c_id INT,
                o_carrier_id INT CHECK (o_carrier_id BETWEEN 1 AND 10 OR o_carrier_id IS NULL),
                o_ol_cnt DECIMAL(2, 0),
                o_all_local DECIMAL(1, 0),
                o_entry_d TIMESTAMP,
                PRIMARY KEY (o_w_id, o_d_id, o_id),
                FOREIGN KEY (o_w_id, o_d_id, o_c_id) REFERENCES customer (c_w_id, c_d_id, c_id)
            );
        """)
        print(f"order table creation ends")
        print(f"order table distribution starts")
        cursor.execute(create_partition_key("order","o_w_id"))
        # cursor.execute(check_partition_key("order"))
        # result = cursor.fetchone()
        # print(result[0] +" partition key: "+ result[1])
        print(f"order table distribution ends")

        # 5. Create item table
        print(f"item table creation starts")
        cursor.execute("""
           CREATE TABLE IF NOT EXISTS item (
                i_id INT,
                i_name VARCHAR(24),
                i_price DECIMAL(5, 2),
                i_im_id INT,
                i_data VARCHAR(50),
                PRIMARY KEY (i_id)
            );
        """)
        print(f"item table creation ends")
        print(f"item table distribution starts")
        cursor.execute(create_partition_key("item","i_id"))
        # cursor.execute(check_partition_key("item"))
        # result = cursor.fetchone()
        # print(result[0] +" partition key: "+ result[1])
        print(f"item table distribution ends")

        
        # 6. Create order_line table
        print(f"order_line table creation starts")
        cursor.execute("""
           CREATE TABLE IF NOT EXISTS order_line (
                ol_w_id INT,
                ol_d_id INT,
                ol_o_id INT,
                ol_number INT,
                ol_i_id INT,
                ol_delivery_d TIMESTAMP ,
                ol_amount DECIMAL(7, 2),
                ol_supply_w_id INT,
                ol_quantity DECIMAL(2, 0),
                ol_dist_info CHAR(24),
                PRIMARY KEY (ol_w_id, ol_d_id, ol_o_id, ol_number)
            );
        """)
        print(f"order_line table creation ends")
        print(f"order_line table distribution starts")
        cursor.execute(create_partition_key("order_line","ol_d_id"))
        # cursor.execute(check_partition_key("order_line"))
        # result = cursor.fetchone()
        # print(result[0] +" partition key: "+ result[1])
        print(f"order_line table distribution ends")

        
        # 6. Create stock table
        print(f"stock table creation starts")
        cursor.execute("""
           CREATE TABLE IF NOT EXISTS stock (
                s_w_id INT,
                s_i_id INT,
                s_quantity DECIMAL(4, 0),
                s_ytd DECIMAL(8, 2),
                s_order_cnt INT,
                s_remote_cnt INT,
                s_dist_01 CHAR(24),
                s_dist_02 CHAR(24),
                s_dist_03 CHAR(24),
                s_dist_04 CHAR(24),
                s_dist_05 CHAR(24),
                s_dist_06 CHAR(24),
                s_dist_07 CHAR(24),
                s_dist_08 CHAR(24),
                s_dist_09 CHAR(24),
                s_dist_10 CHAR(24),
                s_data VARCHAR(50),
                PRIMARY KEY (s_w_id, s_i_id)
            );
        """)
        print(f"stock table creation ends")
        print(f"stock table distribution starts")
        cursor.execute(create_partition_key("stock","s_i_id"))
        # cursor.execute(check_partition_key("stock"))
        # result = cursor.fetchone()
        # print(result[0] +" partition key: "+ result[1])
        print(f"stock table distribution ends")

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


def check_partition_key(table_name):
    return  f"""
                SELECT logicalrelid::regclass AS table_name,
                    attname AS distribution_column
                FROM pg_dist_partition
                JOIN pg_attribute ON attnum = partkey AND attrelid = logicalrelid
                WHERE logicalrelid = '{table_name}'::regclass;
                """

                
def create_partition_key(table_name, key_name):
    return  f"""
               SELECT create_distributed_table('{table_name}','{key_name}');
                """
def main():
    create_tables()

if __name__ == "__main__":
    main()