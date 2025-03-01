import sqlite3

def insert_ihc_results(response_data, db_path):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Ensure the attribution_customer_journey table exists
    cursor.execute('''CREATE TABLE IF NOT EXISTS attribution_customer_journey (
                        conv_id TEXT NOT NULL,
                        session_id TEXT NOT NULL,
                        ihc REAL NOT NULL,
                        PRIMARY KEY(conv_id, session_id)
                      )''')
    conn.commit()
    
    # Insert the IHC data into the attribution_customer_journey table
    for journey in response_data.get('data', []):
        conv_id = journey['conv_id']
        
        for session in journey['sessions']:
            session_id = session['session_id']
            ihc_value = session['ihc']
            
            # Insert or replace the IHC value in the table
            cursor.execute('''INSERT OR REPLACE INTO attribution_customer_journey (conv_id, session_id, ihc) 
                              VALUES (?, ?, ?)''', (conv_id, session_id, ihc_value))
    
    conn.commit()
    conn.close()
    print("IHC results saved to the database!")

