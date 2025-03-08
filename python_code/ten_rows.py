import sqlite3

def print_first_10_rows(db_path, table_name):
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Execute the query to fetch the first 10 rows
        query = f"SELECT * FROM {table_name} LIMIT 10;"
        cursor.execute(query)
        rows = cursor.fetchall()

        if not rows:
            print(f"No data found in {table_name}.")
        else:
            # Print each row
            print(f"\nFirst 10 rows of the {table_name} table:")
            for row in rows:
                print(row)

    except sqlite3.Error as e:
        print(f"SQLite error occurred: {e}")
    finally:
        conn.close()

def main():
    db_path = "../challenge.db"  # Adjust the path if needed
    
    # Ask the user which table they want to print
    print("Which table would you like to print the first 10 rows from?")
    print("1. channel_reporting")
    print("2. attribution_customer_journey")
    print("3. conversions")
    print("4. session_costs")
    print("5. session_sources")
    
    choice = input("Enter 1, 2, 3, 4, or 5: ")

    if choice == "1":
        print_first_10_rows(db_path, "channel_reporting")
    elif choice == "2":
        print_first_10_rows(db_path, "attribution_customer_journey")
    elif choice == "3":
        print_first_10_rows(db_path, "conversions")
    elif choice == "4":
        print_first_10_rows(db_path, "session_costs")
    elif choice == "5":
        print_first_10_rows(db_path, "session_sources")
    else:
        print("Invalid choice. Please select a valid option from 1 to 5.")

if __name__ == "__main__":
    main()
