import sqlite3

def check_ihc_sum_condition(db_path):
    """
    Checks if the sum of 'ihc' column is equal to 1 for each 'conv_id'.
    Also verifies if each row has a unique (conv_id, session_id) combination.
    Prints:
      - Total number of rows
      - Total unique conv_id count
      - Number of conv_id where ihc sum is not 1
      - Number of affected rows
      - If each row has a unique (conv_id, session_id) combination
      - The affected rows themselves (if any)
    """
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Get total number of rows in the table
        cursor.execute("SELECT COUNT(*) FROM attribution_customer_journey;")
        total_rows = cursor.fetchone()[0]

        # Get total number of unique conv_id
        cursor.execute("SELECT COUNT(DISTINCT conv_id) FROM attribution_customer_journey;")
        total_unique_conv_ids = cursor.fetchone()[0]

        # Get conv_id where sum(ihc) != 1
        cursor.execute("""
            SELECT conv_id 
            FROM attribution_customer_journey 
            GROUP BY conv_id 
            HAVING SUM(ihc) != 1;
        """)
        invalid_conv_ids = [row[0] for row in cursor.fetchall()]
        num_invalid_conv_ids = len(invalid_conv_ids)

        # Check if (conv_id, session_id) combinations are unique
        cursor.execute("""
            SELECT COUNT(*) 
            FROM (
                SELECT DISTINCT conv_id, session_id 
                FROM attribution_customer_journey
            );
        """)
        unique_combinations = cursor.fetchone()[0]
        rows_are_unique = unique_combinations == total_rows

        print(f"📊 Total rows in table: {total_rows}")
        print(f"🔢 Total unique conv_id: {total_unique_conv_ids}")
        print(f"⚠️ conv_id where ihc sum ≠ 1 => {num_invalid_conv_ids}")
        
        # Print uniqueness check result
        if rows_are_unique:
            print("✅ Each row has a unique (conv_id, session_id) combination.")
        else:
            print("❌ Duplicate (conv_id, session_id) combinations exist in the table!")

        if invalid_conv_ids:
            # Fetch all rows with invalid conv_id
            cursor.execute("""
                SELECT * FROM attribution_customer_journey
                WHERE conv_id IN ({})
            """.format(",".join("?" * len(invalid_conv_ids))), invalid_conv_ids)
            
            invalid_rows = cursor.fetchall()
            print(f"⚠️ Affected rows: {len(invalid_rows)} out of {total_rows} total rows.\n")

            # for row in invalid_rows:
            #     print(row)
        else:
            print(f"✅ All {total_rows} rows satisfy the ihc sum condition (sum = 1).")

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
    check_ihc_sum_condition(db_path)
    populate_channel_reporting(db_path)
    #print_channel_reporting(db_path)
