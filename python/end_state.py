import os
import csv
import psycopg2

DB_HOST = 'localhost'
DB_PORT = os.getenv('PGPORT', '5115')
DB_NAME = os.getenv('PGDATABASE', 'project')
DB_USER = os.getenv('PGUSER', 'cs4224s')

def main():
    connection = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
    )
    cursor = connection.cursor()

    cursor.execute("SELECT SUM(W_YTD) FROM warehouse")
    sum_w_ytd = cursor.fetchone()[0]

    cursor.execute("SELECT SUM(D_YTD), SUM(D_NEXT_O_ID) FROM district")
    sum_d_ytd, sum_d_next_o_id = cursor.fetchone()

    cursor.execute("""
        SELECT SUM(C_BALANCE), SUM(C_YTD_PAYMENT), SUM(C_PAYMENT_CNT), SUM(C_DELIVERY_CNT) 
        FROM customer
    """)
    sum_c_balance, sum_c_ytd_payment, sum_c_payment_cnt, sum_c_delivery_cnt = cursor.fetchone()

    cursor.execute("""
        SELECT MAX(O_ID), SUM(O_OL_CNT) FROM "order"
    """)
    max_o_id, sum_o_ol_cnt = cursor.fetchone()

    cursor.execute("SELECT SUM(OL_AMOUNT), SUM(OL_QUANTITY) FROM order_line")
    sum_ol_amount, sum_ol_quantity = cursor.fetchone()

    cursor.execute("""
        SELECT SUM(S_QUANTITY), SUM(S_YTD), SUM(S_ORDER_CNT), SUM(S_REMOTE_CNT) 
        FROM Stock
    """)
    sum_s_quantity, sum_s_ytd, sum_s_order_cnt, sum_s_remote_cnt = cursor.fetchone()

    db_state = [
        sum_w_ytd,
        sum_d_ytd,
        sum_d_next_o_id,
        sum_c_balance,
        sum_c_ytd_payment,
        sum_c_payment_cnt,
        sum_c_delivery_cnt,
        max_o_id,
        sum_o_ol_cnt,
        sum_ol_amount,
        sum_ol_quantity,
        sum_s_quantity,
        sum_s_ytd,
        sum_s_order_cnt,
        sum_s_remote_cnt
    ]

    output_file_path = "../output/dbstate.csv"
    with open(output_file_path, mode='w', newline='') as file:
        csv_writer = csv.writer(file)
        for state in db_state:
            csv_writer.writerow([state])


if __name__ == "__main__":
    main()