from transactions_output import test_new_order_xact
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

    test_new_order_xact(c_id, w_id, d_id,o_id, items, cursor)


def payment_xact(c_w_id, c_d_id, c_id, payment, cursor):
    """
    c_w_id: Customer's Warehouse ID
    c_d_id: Customer's District ID
    c_id: Customer ID
    payment: Payment amount
    """
    return test_query(cursor)

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
    return test_query(cursor)

def related_customer_xact(c_w_id, c_d_id, c_id, cursor):
    """
    c_w_id: Customer's Warehouse ID
    c_d_id: Customer's District ID
    c_id: Customer ID
    """
    return test_query(cursor)


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