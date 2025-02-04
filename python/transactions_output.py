def new_order_xact_output(cust_info, tax_info, o_id, details, total_amt, cursor):
    # Display Customer Information
    w_id, d_id, c_id, c_last, c_credit, c_discount = cust_info
    w_tax, d_tax = tax_info
    print(f"Customer Identifier (W_ID, D_ID, C_ID): ({w_id}, {d_id}, {c_id})")
    print(f"Customer Lastname: {c_last}")
    print(f"Customer Credit: {c_credit}")
    print(f"Customer Discount: {c_discount * 100}%")
    print()

    # Display Tax Information
    print(f"Warehouse Tax Rate: {w_tax * 100}%")
    print(f"District Tax Rate: {d_tax * 100}%")
    print()

    cursor.execute("""
        SELECT O_ENTRY_D
        FROM "order"
        WHERE O_W_ID = %s AND O_D_ID = %s AND O_ID = %s
    """, (w_id, d_id, o_id))
    o_entry_d = cursor.fetchone()[0]

    # Display Order Information
    print(f"Order Number: {o_id}")
    print(f"Order Entry Date: {o_entry_d}")
    print(f"Number of Items: {len(details)}")
    print(f"Total Amount for Order: ${total_amt:.2f}")
    print()
    
    for detail in details:
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