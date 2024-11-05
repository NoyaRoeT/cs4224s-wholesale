def new_order_xact_output(c_id, w_id, d_id,o_id, items, cursor):
    # Display Customer Information
    cursor.execute("""
        SELECT C_LAST, C_CREDIT, C_DISCOUNT, W_TAX, D_TAX
        FROM customer, warehouse, district
        WHERE C_W_ID = %s AND C_D_ID = %s AND C_ID = %s
        AND W_ID = %s AND D_W_ID = W_ID AND D_ID = %s;
        """, (w_id, d_id, c_id, w_id, d_id))

    customer_info = cursor.fetchone()
    c_last, c_credit, c_discount, w_tax, d_tax = customer_info
    print(f"Customer Identifier (W_ID, D_ID, C_ID): ({w_id}, {d_id}, {c_id})")
    print(f"Customer Lastname: {c_last}")
    print(f"Customer Credit: {c_credit}")
    print(f"Customer Discount: {c_discount * 100}%")
    print()

    # Display Tax Information
    print(f"Warehouse Tax Rate: {w_tax * 100}%")
    print(f"District Tax Rate: {d_tax * 100}%")
    print()


    query = """
        SELECT 
            ol.OL_I_ID AS ITEM_NUMBER,
            i.I_NAME,
            ol.OL_SUPPLY_W_ID AS SUPPLIER_WAREHOUSE,
            ol.OL_QUANTITY AS QUANTITY,
            ol.OL_AMOUNT,
            s.S_QUANTITY
        FROM 
            order_line ol
        JOIN 
            item i ON ol.OL_I_ID = i.I_ID
        JOIN 
            stock s ON ol.OL_SUPPLY_W_ID = s.S_W_ID AND ol.OL_I_ID = s.S_I_ID
        WHERE 
            ol.OL_O_ID = %s AND ol.OL_D_ID = %s AND ol.OL_W_ID = %s;
    """

    # Execute the query with provided order ID, district ID, and warehouse ID
    cursor.execute(query, (o_id, d_id, w_id))

    # Fetch all results
    order_details = cursor.fetchall()


    # Display Order Information
    print(f"Order Number: {o_id}")
    # print(f"Order Entry Date: {o_entry_d}")
    print(f"Number of Items: {len(items)}")
    print(f"Total Amount for Order: ${len(order_details):.2f}")
    print()
    
    # Print the order details
    print("Order Details:")
    for detail in order_details:
        print(f"ITEM_NUMBER: {detail[0]}")
        print(f"Item Name: {detail[1]}")
        print(f"Supplier Warehouse: {detail[2]}")
        print(f"Quantity: {detail[3]}")
        print(f"Order Line Amount: ${detail[4]:.2f}")
        print(f"Stock Quantity after Order: {detail[5]}")
        print()


def payment_xact_output(c_w_id, c_d_id, c_id,payment,cursor):
    cursor.execute("""
        SELECT C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP, C_PHONE, 
                C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, C_BALANCE
        FROM customer
        WHERE C_W_ID = %s AND C_D_ID = %s AND C_ID = %s;
    """, (c_w_id, c_d_id, c_id))
    customer_info = cursor.fetchone()

    # 5. Retrieve and output the warehouse's address
    cursor.execute("""
        SELECT W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP
        FROM warehouse
        WHERE W_ID = %s;
    """, (c_w_id,))
    warehouse_info = cursor.fetchone()

    # 6. Retrieve and output the district's address
    cursor.execute("""
        SELECT D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP
        FROM district
        WHERE D_W_ID = %s AND D_ID = %s;
    """, (c_w_id, c_d_id))
    district_info = cursor.fetchone()

    # Output the results
    print(f"Customer's ID: ({c_w_id}, {c_d_id}, {c_id})")
    print(f"Name: {customer_info[0]} {customer_info[1]} {customer_info[2]}")
    print(f"Address: {customer_info[3]}, {customer_info[4]}, {customer_info[5]}, {customer_info[6]}, {customer_info[7]}")
    print(f"Phone: {customer_info[8]}, Since: {customer_info[9]}, Credit: {customer_info[10]}")
    print(f"Credit Limit: {customer_info[11]}, Discount: {customer_info[12]}, Balance: {customer_info[13]}")
    print(f"Payment Amount: {payment}")
    
    print("\nWarehouse Address:")
    print(f"{warehouse_info[0]}, {warehouse_info[1]}, {warehouse_info[2]}, {warehouse_info[3]}, {warehouse_info[4]}")
    
    print("\nDistrict Address:")
    print(f"{district_info[0]}, {district_info[1]}, {district_info[2]}, {district_info[3]}, {district_info[4]}")

def related_customer_xact_output(c_w_id, c_d_id, c_id, related_custs):
    print(f"Customer Identifier (W_ID, D_ID, C_ID): ({c_w_id}, {c_d_id}, {c_id})")

    print()

    print(f"{'Warehouse ID':<15} {'District ID':<15} {'Customer ID':<15}")
    print("-" * 45)  # Print a separator line

    for row in related_custs:
        w_id, d_id, c_id = row
        print(f"{w_id:<15} {d_id:<15} {c_id:<15}")
    print()