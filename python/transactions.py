def test_query(cursor):
    cursor.execute("SELECT w_id, w_name, w_city, w_state FROM warehouse LIMIT 5;")

def new_order_xact(c_id, w_id, d_id, items, cursor):
    """
    c_id: Customer ID
    w_id: Warehouse ID
    d_id: District ID
    m: Number of items
    items: List of items, where each item is a tuple of (OL_I_ID, OL_SUPPLY_W_ID, OL_QUANTITY)
    """
    print(f"New Order Transaction: C_ID={c_id}, W_ID={w_id}, D_ID={d_id}")
    for item in items:
        ol_i_id, ol_supply_w_id, ol_quantity = item
        print(f"Item - OL_I_ID: {ol_i_id}, OL_SUPPLY_W_ID: {ol_supply_w_id}, OL_QUANTITY: {ol_quantity}")


def payment_xact(c_w_id, c_d_id, c_id, payment, cursor):
    """
    c_w_id: Customer's Warehouse ID
    c_d_id: Customer's District ID
    c_id: Customer ID
    payment: Payment amount
    """
    print(f"Payment Transaction: C_W_ID={c_w_id}, C_D_ID={c_d_id}, C_ID={c_id}, PAYMENT={payment}")


def delivery_xact(w_id, carrier_id, cursor):
    """
    w_id: Warehouse ID
    carrier_id: Carrier ID
    """
    print(f"Delivery Transaction: W_ID={w_id}, CARRIER_ID={carrier_id}")


def order_status_xact(c_w_id, c_d_id, c_id, cursor):
    """
    c_w_id: Customer's Warehouse ID
    c_d_id: Customer's District ID
    c_id: Customer ID
    """
    print(f"Order-Status Transaction: C_W_ID={c_w_id}, C_D_ID={c_d_id}, C_ID={c_id}")


def stock_level_xact(w_id, d_id, t, l, cursor):
    """
    w_id: Warehouse ID
    d_id: District ID
    t: Threshold
    l: Number of last orders to be examined
    """
    print(f"Stock-Level Transaction: W_ID={w_id}, D_ID={d_id}, T={t}, L={l}")


def popular_item_xact(w_id, d_id, l, cursor):
    """
    w_id: Warehouse ID
    d_id: District ID
    l: Number of last orders to be examined
    """
    print(f"Popular-Item Transaction: W_ID={w_id}, D_ID={d_id}, L={l}")


def top_balance_xact(cursor):
    print(f"Top-Balance Transaction")


def related_customer_xact(c_w_id, c_d_id, c_id, cursor):
    """
    c_w_id: Customer's Warehouse ID
    c_d_id: Customer's District ID
    c_id: Customer ID
    """
    print(f"Related-Customer Transaction: C_W_ID={c_w_id}, C_D_ID={c_d_id}, C_ID={c_id}")



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