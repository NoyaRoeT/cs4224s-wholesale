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
    return test_query(cursor)


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