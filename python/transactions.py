from transactions_output import *
from decimal import Decimal

def test_query(cursor):
    cursor.execute("SELECT w_id, w_name, w_city, w_state FROM warehouse LIMIT 5;")
    return cursor.fetchall()

    
def new_order_xact(c_id, w_id, d_id, items, cursor):
    """
    c_id: Customer ID
    w_id: Warehouse ID
    d_id: District ID
    m: Number of items
    items: List of items, where each item is a tuple of (OL_I_ID, OL_SUPPLY_W_ID, OL_QUANTITY)
    """
    cursor.execute("""
            SELECT D_NEXT_O_ID FROM district 
            WHERE D_W_ID = %s AND D_ID = %s FOR UPDATE;
        """, (w_id, d_id))
    
    # get D_NEXT_O_ID
    o_id = cursor.fetchone()[0]

    # increase D_NEXT_O_ID by 1
    cursor.execute("""
    UPDATE district SET D_NEXT_O_ID = D_NEXT_O_ID + 1 
    WHERE D_W_ID = %s AND D_ID = %s;
            """, (w_id, d_id))
    
    # Start by assuming all items are local
    all_local = 1
    total_amount = 0
    # Process each item
    for i in range(len(items)):
        item_number = items[i][0]
        supplier_warehouse = items[i][1]
        quantity =Decimal(items[i][2])

        # If any item is from a remote warehouse, set all_local to 0
        if supplier_warehouse != w_id:
            all_local = 0

        cursor.execute("""
            SELECT I_PRICE, S_QUANTITY, S_YTD, S_ORDER_CNT, S_REMOTE_CNT 
            FROM item, stock 
            WHERE I_ID = %s AND S_W_ID = %s AND S_I_ID = I_ID;
            """, (item_number, supplier_warehouse))
        
        item_info = cursor.fetchone()
        # Let S_QUANTITY denote the stock quantity for item ITEM_NUMBER[i] and warehouse SUPPLIER_WAREHOUSE[i]
        i_price, s_quantity, s_ytd, s_order_cnt, s_remote_cnt = item_info
        adjusted_qty = s_quantity - quantity
        if adjusted_qty < 10:
            adjusted_qty += 100

        # Update stock with new quantity, increment counters
        cursor.execute("""
            UPDATE stock 
            SET S_QUANTITY = %s, S_YTD = S_YTD + %s, S_ORDER_CNT = S_ORDER_CNT + 1, S_REMOTE_CNT = S_REMOTE_CNT + %s
            WHERE S_W_ID = %s AND S_I_ID = %s;
            """, (adjusted_qty, quantity, supplier_warehouse, item_number,1 if supplier_warehouse != w_id else 0 ))
        
        # Calculate the amount for this order line
        item_amount = quantity * i_price
        total_amount += item_amount

        # Insert into order-line
        cursor.execute("""
            INSERT INTO order_line (OL_O_ID, OL_D_ID, OL_W_ID, OL_NUMBER, OL_I_ID, OL_SUPPLY_W_ID, OL_QUANTITY, OL_AMOUNT, OL_DELIVERY_D,OL_DIST_INFO)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NULL,'S_DIST_'||%s);
        """, (o_id, d_id, w_id, i+1, item_number, supplier_warehouse, quantity, item_amount,w_id))

        # items[i]+=(adjusted_qty,)
    cursor.execute("""
        SELECT C_DISCOUNT, W_TAX, D_TAX
        FROM customer, warehouse, district
        WHERE C_W_ID = %s AND C_D_ID = %s AND C_ID = %s
        AND W_ID = %s AND D_W_ID = W_ID AND D_ID = %s;
    """, (w_id, d_id, c_id, w_id, d_id))

    customer_info = cursor.fetchone()
    c_discount, w_tax, d_tax = customer_info
    total_amount = total_amount * (1 + w_tax + d_tax) * (1 - c_discount)    
    # Insert the order
    cursor.execute("""
        INSERT INTO "order" (O_ID, O_D_ID, O_W_ID, O_C_ID, O_ENTRY_D, O_OL_CNT, O_ALL_LOCAL)
        VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP, %s, %s);
    """, (o_id, d_id, w_id, c_id, len(items), all_local))

    new_order_xact_output(c_id, w_id, d_id,o_id, items, cursor)

def payment_xact(c_w_id, c_d_id, c_id, payment, cursor):
    """
    c_w_id: Customer's Warehouse ID
    c_d_id: Customer's District ID
    c_id: Customer ID
    payment: Payment amount
    """
    cursor.execute("""
        UPDATE warehouse
        SET W_YTD = W_YTD + %s
        WHERE W_ID = %s;
        """, (payment, c_w_id))
    
    cursor.execute("""
        UPDATE district
        SET D_YTD = D_YTD + %s
        WHERE D_W_ID = %s AND D_ID = %s;
        """, (payment, c_w_id, c_d_id))
    
    cursor.execute("""
        UPDATE customer
        SET C_BALANCE = C_BALANCE - %s,
            C_YTD_PAYMENT = C_YTD_PAYMENT + %s,
            C_PAYMENT_CNT = C_PAYMENT_CNT + 1
        WHERE C_W_ID = %s AND C_D_ID = %s AND C_ID = %s;
        """, (payment, payment, c_w_id, c_d_id, c_id))
    payment_xact_output(c_w_id, c_d_id, c_id, payment, cursor)

def delivery_xact(w_id, carrier_id, cursor):
    """
    w_id: Warehouse ID
    carrier_id: Carrier ID
    """
    return test_query(cursor)

def order_status_xact(c_w_id, c_d_id, c_id, cursor):
    """
    c_w_id: Customer's Warehouse ID
    c_d_id: Customer's District ID
    c_id: Customer ID
    """
    return test_query(cursor)

def stock_level_xact(w_id, d_id, t, l, cursor):
    """
    w_id: Warehouse ID
    d_id: District ID
    t: Threshold
    l: Number of last orders to be examined
    """
    return test_query(cursor)

def popular_item_xact(w_id, d_id, l, cursor):
    """
    w_id: Warehouse ID
    d_id: District ID
    l: Number of last orders to be examined
    """
    return test_query(cursor)

def top_balance_xact(cursor):
    top_bal_query = """
        SELECT CONCAT(C_FIRST, ' ', C_MIDDLE, ' ', C_LAST) as cust_name,
            C_BALANCE as balance,
            W_NAME as w_name,
            D_NAME as d_name
        FROM customer
        JOIN district ON C_D_ID = D_ID AND C_W_ID = D_W_ID
        JOIN warehouse on D_W_ID = W_ID
        ORDER BY C_BALANCE DESC
        LIMIT 10;
    """
    cursor.execute(top_bal_query)
    results = cursor.fetchall()

    # Print the results
    print(f"{'Customer Name':<30} {'Balance':<15} {'Warehouse':<20} {'District':<20}")
    print("=" * 85)  # Print a separator line

    for row in results:
        cust_name, balance, w_name, d_name = row
        print(f"{cust_name:<30} {balance:<15} {w_name:<20} {d_name:<20}")
    print()

def related_customer_xact(c_w_id, c_d_id, c_id, cursor):
    """
    c_w_id: Customer's Warehouse ID
    c_d_id: Customer's District ID
    c_id: Customer ID
    """
    # Get data of specified customer
    query = """
        SELECT C_STATE
        FROM customer
        WHERE C_W_ID = %s AND C_D_ID = %s AND C_ID = %s
    """
    cursor.execute(query, (c_w_id, c_d_id, c_id))
    results = cursor.fetchone()

    if not results:
        return related_customer_xact_output(c_w_id, c_d_id, c_id, [])
    else:
        c_state = results[0]

    query = """
        SELECT O_W_ID, O_D_ID, O_ID
        FROM "order"
        WHERE O_W_ID = %s AND O_D_ID = %s AND O_C_ID = %s
        ORDER BY O_ENTRY_D DESC
        LIMIT 1
    """
    cursor.execute(query, (c_w_id, c_d_id, c_id))
    results = cursor.fetchone()
    if not results:
        return related_customer_xact_output(c_w_id, c_d_id, c_id, [])
    o_w_id, o_d_id, o_id = results

    query = """
        SELECT OL_I_ID
        FROM order_line
        WHERE OL_W_ID = %s AND OL_D_ID = %s AND OL_O_ID = %s
    """
    cursor.execute(query, (o_w_id, o_d_id, o_id))
    cust_items_set = {item[0] for item in cursor.fetchall()}
    if not cust_items_set:
        return related_customer_xact_output(c_w_id, c_d_id, c_id, [])

    # Get data for all customers of same state
    query = """
        WITH last_orders AS (
            SELECT a.O_W_ID, a.O_D_ID, a.O_C_ID, a.O_ID
            FROM "order" a
            JOIN (
                SELECT O_W_ID, O_D_ID, O_C_ID, MAX(O_ENTRY_D) as O_ENTRY_D
                FROM "order"
                GROUP BY O_W_ID, O_D_ID, O_C_ID 
            ) b 
            ON a.O_W_ID = b.O_W_ID AND a.O_D_ID = b.O_D_ID AND a.O_C_ID = b.O_C_ID AND a.O_ENTRY_D = b.O_ENTRY_D
        ),
        last_order_items AS (
            SELECT O_W_ID, O_D_ID, O_ID, O_C_ID, OL_I_ID
            FROM last_orders JOIN order_line
            ON OL_W_ID = O_W_ID AND OL_D_ID = O_D_ID AND OL_O_ID = O_ID
        )
        SELECT C_W_ID, C_D_ID, C_ID, OL_I_ID
        FROM customer
        JOIN last_order_items
        ON C_W_ID = O_W_ID AND C_D_ID = O_D_ID AND C_ID = O_C_ID
        WHERE C_STATE = %s
        AND NOT (C_W_ID = %s AND C_D_ID = %s AND C_ID = %s)
        ORDER BY C_W_ID, C_D_ID, C_ID
    """
    cursor.execute(query, (c_state, c_w_id, c_d_id, c_id))
    other_custs = cursor.fetchall()
    if not other_custs:
        return related_customer_xact_output(c_w_id, c_d_id, c_id, [])

    # Record number of same items
    cust_scores = {}
    for other_w_id, other_d_id, other_c_id, other_i_id in other_custs:
        if other_i_id in cust_items_set:
            cust_key = (other_w_id, other_d_id, other_c_id)
            if cust_key not in cust_scores:
                cust_scores[cust_key] = 0
            cust_scores[cust_key] += 1
    
    for cust_key, score in cust_scores.items():
        print(f"Customer: {cust_key}, Score: {score}")

    related_custs = [key for key, count in cust_scores.items() if count >= 2]
    return related_customer_xact_output(c_w_id, c_d_id, c_id, related_custs)


xact_dict = {
    'N': new_order_xact,
    'P': payment_xact,
    'D': delivery_xact,
    'O': order_status_xact,
    'S': stock_level_xact,
    'I': popular_item_xact,
    'T': top_balance_xact,
    'R': related_customer_xact
}

def get_xact_func(xact_key):
    return xact_dict[xact_key]