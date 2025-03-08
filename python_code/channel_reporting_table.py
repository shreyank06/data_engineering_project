import sqlite3

def check_ihc_sum_condition(db_path):
    """
    Checks if the sum of 'ihc' column is equal to 1 for each 'conv_id',
    prints the number of unique 'conv_id', and displays the first 20 rows.
    """
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Count the number of unique conv_id
        cursor.execute("SELECT COUNT(DISTINCT conv_id) FROM attribution_customer_journey;")
        conv_id_count = cursor.fetchone()[0]
        print(f"🔢 Total unique conversion IDs: {conv_id_count}")

        # Query to check if sum(ihc) == 1 for each conv_id
        cursor.execute("""
            SELECT conv_id, SUM(ihc) 
            FROM attribution_customer_journey 
            GROUP BY conv_id 
            HAVING SUM(ihc) != 1;
        """)

        invalid_rows = cursor.fetchall()

        if invalid_rows:
            print("⚠️ The following conversion IDs have ihc sum ≠ 1:")
            for conv_id, ihc_sum in invalid_rows:
                print(f"Conversion ID: {conv_id}, IHC Sum: {ihc_sum}")
        else:
            print("✅ All conversion IDs satisfy the ihc sum condition (sum = 1).")

        # Fetch the first 20 rows
        cursor.execute("SELECT * FROM attribution_customer_journey LIMIT 20;")
        rows = cursor.fetchall()
        
        print("\n📋 First 20 rows of attribution_customer_journey:")
        for row in rows:
            print(row)

    except sqlite3.Error as e:
        print(f"SQLite error occurred: {e}")
    
    finally:
        conn.close()

def populate_channel_reporting(db_path):
    """
    Populates the channel_reporting table by aggregating data from session_sources, 
    session_costs, conversions, and attribution_customer_journey.
    If the table exists, it clears the contents before inserting new data.
    If the table doesn't exist, it creates the table first.
    """
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Check if the table exists
        cursor.execute("""
            SELECT name FROM sqlite_master 
            WHERE type='table' AND name='channel_reporting';
        """)
        table_exists = cursor.fetchone()

        if table_exists:
            print("🗑️ Clearing existing data in channel_reporting...")
            cursor.execute("DELETE FROM channel_reporting;")
        else:
            print("📌 Creating table channel_reporting...")
            cursor.execute("""
                CREATE TABLE channel_reporting (
                    channel_name TEXT,
                    date TEXT,
                    cost REAL,
                    ihc REAL,
                    ihc_revenue REAL
                );
            """)

        # Insert data into channel_reporting
        query = """
        INSERT INTO channel_reporting (channel_name, date, cost, ihc, ihc_revenue)
        SELECT 
            ss.channel_name, 
            ss.event_date AS date, 
            COALESCE(SUM(sc.cost), 0) AS cost, 
            COALESCE(SUM(acj.ihc), 0) AS ihc,
            COALESCE(SUM(acj.ihc * c.revenue), 0) AS ihc_revenue
        FROM session_sources ss
        LEFT JOIN session_costs sc ON ss.session_id = sc.session_id
        LEFT JOIN attribution_customer_journey acj ON ss.session_id = acj.session_id
        LEFT JOIN conversions c ON acj.conv_id = c.conv_id
        GROUP BY ss.channel_name, ss.event_date;
        """

        cursor.execute(query)
        conn.commit()
        print("✅ channel_reporting table populated successfully.")

    except sqlite3.Error as e:
        print(f"SQLite error occurred: {e}")

    finally:
        conn.close()

def print_channel_reporting(db_path):
    """
    Prints the contents of the channel_reporting table, including column names,
    number of rows, and the actual data.
    """
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Fetch and print the column names
        cursor.execute("SELECT * FROM channel_reporting LIMIT 0;")
        column_names = [description[0] for description in cursor.description]
        print("\n📋 Column names in channel_reporting:")
        print(", ".join(column_names))

        # Fetch all rows and print the number of rows
        cursor.execute("SELECT * FROM channel_reporting;")
        rows = cursor.fetchall()
        print(f"\n🔢 Number of rows in channel_reporting: {len(rows)}")

        # Print the rows
        print("\n📋 Contents of channel_reporting:")
        for row in rows:
            print(row)

    except sqlite3.Error as e:
        print(f"SQLite error occurred: {e}")

    finally:
        conn.close()

# Example usage
if __name__ == "__main__":
    db_path = "../challenge.db"  # Adjust path if needed
    #check_ihc_sum_condition(db_path)
    #populate_channel_reporting(db_path)
    print_channel_reporting(db_path)
