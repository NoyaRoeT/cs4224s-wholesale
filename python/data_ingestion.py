import psycopg2
import sys
import argparse
# Currently hard-coded, only works on team account because of DB_USER
DB_HOST = 'xcne0'
DB_PORT = '5115'
DB_NAME = 'project'
DB_USER = 'cs4224s'


def load_order_data(csv_file_path):
    try:
        connection = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
        )
        cursor = connection.cursor()

        # SQL command to copy CSV data into the Citus table
        copy_sql = '''
        COPY warehouse (w_id, w_name, w_street_1, w_street_2, w_city, w_state, w_zip, w_tax, w_ytd)
        FROM STDIN WITH CSV HEADER
        DELIMITER ',';
        '''
        # Open the CSV file and execute the COPY command
        with open(csv_file_path, 'r') as f:
            cursor.copy_expert(copy_sql, f)

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


def main():
    parser = argparse.ArgumentParser("csv path")
    parser.add_argument("--w", type=str, default="")
    args = parser.parse_args()
    if args.w != "":
        load_order_data(args.w)

if __name__ == "__main__":
    main()
