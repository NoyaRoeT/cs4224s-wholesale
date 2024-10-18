def test_new_order_xact(c_id, w_id, d_id,o_id, items, cursor):

    print("Transaction Summary:")
    print("----------------------")
    
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

