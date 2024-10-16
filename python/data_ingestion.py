import psycopg2
import sys
import argparse
# Currently hard-coded, only works on team account because of DB_USER
DB_HOST = 'xcne0'
DB_PORT = '5115'
DB_NAME = 'project'
DB_USER = 'cs4224s'


def load_warehouse_data(csv_file_path,cursor):
    try:

        print(f"warehouse data ingestion starts")
        # SQL command to copy CSV data into the Citus table
        copy_sql = '''
        COPY warehouse (w_id, w_name, w_street_1, w_street_2, w_city, w_state, w_zip, w_tax, w_ytd)
        FROM STDIN WITH CSV
        DELIMITER ',';
        '''
        # Open the CSV file and execute the COPY command
        with open(csv_file_path, 'r') as f:
            cursor.copy_expert(copy_sql, f)

        cursor.execute("SELECT * FROM warehouse;")
        warehouses = cursor.fetchall()
        print("warehouses in the table:")
        for warehouse in warehouses:
            print(warehouse)

        print(f"warehouse data ingestion ends")
    except Exception as e:
        print(f"An error occurred: {e}")



def load_district_data(csv_file_path,cursor):
    try:

        print(f"district data ingestion starts")
        # SQL command to copy CSV data into the Citus table
        copy_sql = '''
        COPY district (d_w_id, d_id, d_name, d_street_1, d_street_2, d_city, d_state, d_zip, d_tax, d_ytd, d_next_o_id)
        FROM STDIN WITH CSV
        DELIMITER ',';
        '''
        # Open the CSV file and execute the COPY command
        with open(csv_file_path, 'r') as f:
            cursor.copy_expert(copy_sql, f)

        cursor.execute("SELECT * FROM district;")
        districts = cursor.fetchall()
        print("districts in the table:")
        for district in districts:
            print(district)

        print(f"district data ingestion ends")
    except Exception as e:
        print(f"An error occurred: {e}")

def load_customer_data(csv_file_path,cursor):
    try:

        print(f"customer data ingestion starts")
        # SQL command to copy CSV data into the Citus table
        copy_sql = '''
        COPY customer (c_w_id, c_d_id, c_id, c_first, c_middle, c_last, c_street_1, c_street_2, c_city, c_state, c_zip, c_phone, c_since, c_credit, c_credit_lim, c_discount, c_balance, c_ytd_payment, c_payment_cnt, c_delivery_cnt, c_data)
        FROM STDIN WITH CSV
        DELIMITER ',';
        '''
        # Open the CSV file and execute the COPY command
        with open(csv_file_path, 'r') as f:
            cursor.copy_expert(copy_sql, f)

        print(f"customer data ingestion ends")
    except Exception as e:
        print(f"An error occurred: {e}")

def load_order_data(csv_file_path,cursor):
    try:

        print(f"order data ingestion starts")
        # SQL command to copy CSV data into the Citus table
        copy_sql = '''
        COPY "order" (o_w_id, o_d_id, o_id, o_c_id, o_carrier_id, o_ol_cnt, o_all_local, o_entry_d)
        FROM STDIN WITH CSV NULL AS 'null'
        DELIMITER ',';
        '''
        # Open the CSV file and execute the COPY command
        with open(csv_file_path, 'r') as f:
            cursor.copy_expert(copy_sql, f)

        print(f"order data ingestion ends")
    except Exception as e:
        print(f"An error occurred: {e}")



def load_item_data(csv_file_path,cursor):
    try:

        print(f"item data ingestion starts")
        # SQL command to copy CSV data into the Citus table
        copy_sql = '''
        COPY item (i_id, i_name, i_price, i_im_id, i_data)
        FROM STDIN WITH CSV
        DELIMITER ',';
        '''
        # Open the CSV file and execute the COPY command
        with open(csv_file_path, 'r') as f:
            cursor.copy_expert(copy_sql, f)

        print(f"item data ingestion ends")
    except Exception as e:
        print(f"An error occurred: {e}")


def load_order_line_data(csv_file_path,cursor):
    try:

        print(f"order_line data ingestion starts")
        # SQL command to copy CSV data into the Citus table
        copy_sql = '''
        COPY order_line (ol_w_id, ol_d_id, ol_o_id, ol_number, ol_i_id, ol_delivery_d, ol_amount, ol_supply_w_id, ol_quantity, ol_dist_info)
        FROM STDIN WITH CSV NULL AS 'null'
        DELIMITER ',';
        '''
        # Open the CSV file and execute the COPY command
        with open(csv_file_path, 'r') as f:
            cursor.copy_expert(copy_sql, f)

        print(f"order_line data ingestion ends")
    except Exception as e:
        print(f"An error occurred: {e}")


def load_stock_data(csv_file_path,cursor):
    try:

        print(f"stock data ingestion starts")
        # SQL command to copy CSV data into the Citus table
        copy_sql = '''
        COPY stock (s_w_id, s_i_id, s_quantity, s_ytd, s_order_cnt, s_remote_cnt, s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05, s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10, s_data)
        FROM STDIN WITH CSV
        DELIMITER ',';
        '''
        # Open the CSV file and execute the COPY command
        with open(csv_file_path, 'r') as f:
            cursor.copy_expert(copy_sql, f)

        print(f"stock data ingestion ends")
    except Exception as e:
        print(f"An error occurred: {e}")

def main():
    parser = argparse.ArgumentParser("csv path")
    parser.add_argument("--w", type=str, default="")
    parser.add_argument("--d", type=str, default="")
    parser.add_argument("--c", type=str, default="")
    parser.add_argument("--o", type=str, default="")
    parser.add_argument("--i", type=str, default="")
    parser.add_argument("--ol", type=str, default="")
    parser.add_argument("--s", type=str, default="")
    args = parser.parse_args()
    try:
        connection = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
        )
        cursor = connection.cursor()

        if args.w != "":
            load_warehouse_data(args.w,cursor)
        if args.d != "":
            load_district_data(args.d,cursor)
        if args.c != "":
            load_customer_data(args.c,cursor)
        if args.o != "":
            load_order_data(args.o,cursor)
        if args.i != "":
            load_item_data(args.i,cursor)
        if args.ol != "":
            load_order_line_data(args.ol,cursor)
        if args.s != "":
            load_stock_data(args.s,cursor)
        connection.commit()

    except Exception as e:
        print(f"An error occurred: {e}")

    finally:
        # Close the cursor and connection
        if cursor:
            cursor.close()
        if connection:
            connection.close()

if __name__ == "__main__":
    main()
