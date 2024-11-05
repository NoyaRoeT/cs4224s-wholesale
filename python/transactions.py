from transactions_output import *
from decimal import Decimal
from datetime import datetime
    
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
    # Loop through each district from 1 to 10
    for district_no in range(1, 11):
        print(f"Processing delivery for warehouse {w_id}, district {district_no}")

        # Step 1: Find the oldest unfulfilled order in this district
        cursor.execute("""
            SELECT O_ID, O_C_ID 
            FROM "order" 
            WHERE O_W_ID = %s AND O_D_ID = %s AND O_CARRIER_ID IS NULL 
            ORDER BY O_ID ASC 
            LIMIT 1;
        """, (w_id, district_no))
        
        result = cursor.fetchone()
        print(f"Oldest unfulfilled order in district {district_no}: {result}")

        # If there's no unfulfilled order, skip to the next district
        if not result:
            print(f"No unfulfilled orders found for district {district_no}")
            continue
        
        o_id, c_id = result

        # Step 2: Update the order by setting O_CARRIER_ID to carrier_id
        cursor.execute("""
            UPDATE "order" 
            SET O_CARRIER_ID = %s 
            WHERE O_W_ID = %s AND O_D_ID = %s AND O_ID = %s;
        """, (carrier_id, w_id, district_no, o_id))
        print(f"Updated order {o_id} in district {district_no} with carrier ID {carrier_id}")

        # Step 3: Update all order lines to set OL_DELIVERY_D to the current date/time
        current_time = datetime.now()
        cursor.execute("""
            UPDATE order_line 
            SET OL_DELIVERY_D = %s 
            WHERE OL_W_ID = %s AND OL_D_ID = %s AND OL_O_ID = %s;
        """, (current_time, w_id, district_no, o_id))
        print(f"Updated delivery date for order lines of order {o_id} to {current_time}")

        # Step 4: Calculate the total amount for the order and update the customer's balance
        cursor.execute("""
            SELECT SUM(OL_AMOUNT) 
            FROM order_line 
            WHERE OL_W_ID = %s AND OL_D_ID = %s AND OL_O_ID = %s;
        """, (w_id, district_no, o_id))
        
        total_amount = cursor.fetchone()[0] or 0
        print(f"Total amount for order {o_id}: {total_amount}")

        # Step 5: Update the customer's balance and delivery count
        cursor.execute("""
            UPDATE customer 
            SET C_BALANCE = C_BALANCE + %s, 
                C_DELIVERY_CNT = C_DELIVERY_CNT + 1 
            WHERE C_W_ID = %s AND C_D_ID = %s AND C_ID = %s;
        """, (total_amount, w_id, district_no, c_id))
        print(f"Updated customer {(w_id, district_no, c_id)} with new balance and delivery count")

def order_status_xact(c_w_id, c_d_id, c_id, cursor):
    """
    c_w_id: Customer's Warehouse ID
    c_d_id: Customer's District ID
    c_id: Customer ID
    """
    # Step 1: Get the customer's name and balance
    cursor.execute("""
        SELECT C_FIRST, C_MIDDLE, C_LAST, C_BALANCE
        FROM customer
        WHERE C_W_ID = %s AND C_D_ID = %s AND C_ID = %s;
    """, (c_w_id, c_d_id, c_id))
    customer_info = cursor.fetchone()
    c_first, c_middle, c_last, c_balance = customer_info
    
    print(f"Customer Name: {c_first} {c_middle} {c_last}, Balance: {c_balance}")
    
    # Step 2: Get the last order for the customer
    cursor.execute("""
        SELECT O_ID, O_ENTRY_D, O_CARRIER_ID
        FROM "order"
        WHERE O_W_ID = %s AND O_D_ID = %s AND O_C_ID = %s
        ORDER BY O_ID DESC
        LIMIT 1;
    """, (c_w_id, c_d_id, c_id))
    order_info = cursor.fetchone()

    if not order_info:
        print(f"No orders found for customer {c_id}")
        return

    o_id, o_entry_d, o_carrier_id = order_info
    print(f"Last Order ID: {o_id}, Entry Date: {o_entry_d}, Carrier ID: {o_carrier_id}")

    # Step 3: Get order line items for the last order
    cursor.execute("""
        SELECT OL_I_ID, OL_SUPPLY_W_ID, OL_QUANTITY, OL_AMOUNT, OL_DELIVERY_D
        FROM order_line
        WHERE OL_W_ID = %s AND OL_D_ID = %s AND OL_O_ID = %s;
    """, (c_w_id, c_d_id, o_id))
    order_lines = cursor.fetchall()

    for line in order_lines:
        ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_delivery_d = line
        print(f"Item ID: {ol_i_id}, Supply Warehouse: {ol_supply_w_id}, Quantity: {ol_quantity}, Amount: {ol_amount}, Delivery Date: {ol_delivery_d}")

def stock_level_xact(w_id, d_id, t, l, cursor):
    """
    w_id: Warehouse ID
    d_id: District ID
    t: Threshold
    l: Number of last orders to be examined
    """
    # Step 1: Get the next available order ID for the district
    cursor.execute("""
        SELECT D_NEXT_O_ID
        FROM district
        WHERE D_W_ID = %s AND D_ID = %s;
    """, (w_id, d_id))
    d_next_o_id = cursor.fetchone()[0]

    # Step 2: Get items from the last L orders
    cursor.execute("""
        SELECT DISTINCT OL_I_ID
        FROM order_line
        WHERE OL_W_ID = %s AND OL_D_ID = %s AND OL_O_ID >= %s AND OL_O_ID < %s;
    """, (w_id, d_id, d_next_o_id - l, d_next_o_id))
    items = cursor.fetchall()

    # Step 3: Count items where stock quantity is below the threshold
    low_stock_count = 0
    for (ol_i_id,) in items:
        cursor.execute("""
            SELECT S_QUANTITY
            FROM stock
            WHERE S_W_ID = %s AND S_I_ID = %s;
        """, (w_id, ol_i_id))
        s_quantity = cursor.fetchone()[0]
        if s_quantity < t:
            low_stock_count += 1

    print(f"Number of items with stock below threshold {t}: {low_stock_count}")

def popular_item_xact(w_id, d_id, l, cursor):
    """
    w_id: Warehouse ID
    d_id: District ID
    l: Number of last orders to be examined
    """
    # Step 1: Get the next available order ID for the district
    cursor.execute("""
        SELECT D_NEXT_O_ID
        FROM district
        WHERE D_W_ID = %s AND D_ID = %s;
    """, (w_id, d_id))
    d_next_o_id = cursor.fetchone()[0]

    # Step 2: Get items and their quantities from the last L orders
    cursor.execute("""
        SELECT OL_I_ID, SUM(OL_QUANTITY) as total_qty, COUNT(DISTINCT OL_O_ID) as num_orders
        FROM order_line
        WHERE OL_W_ID = %s AND OL_D_ID = %s AND OL_O_ID >= %s AND OL_O_ID < %s
        GROUP BY OL_I_ID
        ORDER BY total_qty DESC, num_orders DESC, OL_I_ID ASC
        LIMIT 5;
    """, (w_id, d_id, d_next_o_id - int(l), d_next_o_id))
    popular_items = cursor.fetchall()

    print(f"District (W_ID={w_id}, D_ID={d_id}), Last {l} Orders:")
    for ol_i_id, total_qty, num_orders in popular_items:
        # Fetch item details
        cursor.execute("""
            SELECT I_NAME, I_PRICE
            FROM item
            WHERE I_ID = %s;
        """, (ol_i_id,))
        i_name, i_price = cursor.fetchone()
        
        print(f"Item ID: {ol_i_id}, Name: {i_name}, Price: {i_price}, Total Quantity: {total_qty}, Number of Orders: {num_orders}")

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

    c_state = results[0]    
    
    # Get item set for last order of specified customer (should be using the index)
    query = """
        SELECT O_W_ID, O_D_ID, O_ID
        FROM "order"
        WHERE O_W_ID = %s AND O_D_ID = %s AND O_C_ID = %s
        ORDER BY O_ENTRY_D DESC
        LIMIT 1;
    """
    cursor.execute(query, (c_w_id, c_d_id, c_id))
    last_order = cursor.fetchone()

    if not last_order:
        return related_customer_xact_output(c_w_id, c_d_id, c_id, [])
    
    query = """
        SELECT OL_I_ID
        FROM order_line
        WHERE OL_W_ID = %s AND OL_D_ID = %s AND OL_O_ID = %s
    """
    cursor.execute(query, (last_order[0], last_order[1], last_order[2]))
    cust_items_set = {row[0] for row in cursor.fetchall()}

    # Get all customers of same state
    query = """
        SELECT C_W_ID, C_D_ID, C_ID
        FROM customer
        WHERE C_STATE = %s
        AND NOT (C_W_ID = %s AND C_D_ID = %s AND C_ID = %s)
    """
    cursor.execute(query, (c_state, c_w_id, c_d_id, c_id))
    same_state_custs = cursor.fetchall()

    if not same_state_custs:
        return related_customer_xact_output(c_w_id, c_d_id, c_id, [])
    
    # Find score for customers of same state
    cust_scores = {}
    for cust in same_state_custs:
        cust_key = (cust[0], cust[1], cust[2])
        query = """
            SELECT O_W_ID, O_D_ID, O_ID
            FROM "order"
            WHERE O_W_ID = %s AND O_D_ID = %s AND O_C_ID = %s
            ORDER BY O_ENTRY_D DESC
            LIMIT 1;
        """
        cursor.execute(query, cust_key)
        last_order = cursor.fetchone()

        query = """
            SELECT OL_I_ID
            FROM order_line
            WHERE OL_W_ID = %s AND OL_D_ID = %s AND OL_O_ID = %s
        """
        cursor.execute(query, (last_order[0], last_order[1], last_order[2]))
        cust_items = cursor.fetchall()

        for item in cust_items:
            item_id = item[0]

            if item_id in cust_items_set:
                if cust_key not in cust_scores:
                    cust_scores[cust_key] = 0
                cust_scores[cust_key] += 1

    related_custs = [key for key, count in cust_scores.items() if count >= 2]
    sorted_related_custs = sorted(related_custs)
    return related_customer_xact_output(c_w_id, c_d_id, c_id, sorted_related_custs)


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