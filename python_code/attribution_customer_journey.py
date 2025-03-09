import sqlite3

def create_attribution_customer_journey_table(db_path):
    """
    Creates the attribution_customer_journey table if it doesn't exist.
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    print("Checking if 'attribution_customer_journey' table exists...")

    # Create table if not exists without PRIMARY KEY
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS attribution_customer_journey (
        conv_id TEXT NOT NULL,
        session_id TEXT NOT NULL,
        ihc REAL NOT NULL CHECK (ihc >= 0 AND ihc <= 1)
    )
    """)

    conn.commit()
    conn.close()
    print("Table 'attribution_customer_journey' is ready.")


def clear_attribution_customer_journey_table(db_path):
    """
    Clears the contents of the attribution_customer_journey table.
    """
    print('Clearing the table...')
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Delete all rows from the table
    cursor.execute("DELETE FROM attribution_customer_journey")

    conn.commit()
    conn.close()
    print("Table cleared.")


def insert_ihc_results(response_data, db_path):
    """
    Inserts the IHC API results into the database.
    """
    print("Inserting IHC results into the database...")

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    for result in response_data.get("value", []):
        conv_id = result.get("conversion_id")
        ihc_value = result.get("ihc", 0)
        session_id = result.get("session_id")

        # Insert the data into the table
        try:
            cursor.execute("""
                INSERT INTO attribution_customer_journey (conv_id, session_id, ihc) 
                VALUES (?, ?, ?)
            """, (conv_id, session_id, ihc_value))
            print(f"Inserted session {session_id} for conversion {conv_id}")
        except sqlite3.Error as e:
            print(f"Error inserting data for session {session_id}: {e}")
        
        # Fetch and print the row just inserted
        cursor.execute("""
            SELECT * FROM attribution_customer_journey 
            WHERE conv_id = ? AND session_id = ?
        """, (conv_id, session_id))
        
        inserted_row = cursor.fetchone()
        print("Inserted row:", inserted_row)
        
    conn.commit()
    conn.close()
    print("IHC results successfully inserted.")
