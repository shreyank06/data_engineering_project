import sqlite3
import pandas as pd
from send_to_ihc_api import send_to_ihc_api_and_store_results  # Importing the function from send_to_ihc.py
import os
import json
from channel_reporting_excel import main as channel_reporting_main

def json_serial(obj):
    """Custom JSON serializer for objects not serializable by default."""
    if isinstance(obj, pd.Timestamp):
        return obj.isoformat()  # Convert Timestamp to ISO 8601 string format
    raise TypeError("Type not serializable")

def check_channel_reporting_table_exists(db_path):
    """Check if the channel_reporting table exists in the database."""
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Query to check if channel_reporting table exists
        cursor.execute("""
            SELECT name FROM sqlite_master WHERE type='table' AND name='channel_reporting';
        """)
        table_exists = cursor.fetchone()

        conn.close()
        return table_exists is not None  # Returns True if the table exists, else False

    except sqlite3.Error as e:
        print(f"SQLite error occurred: {e}")
        return False

def get_customer_journeys(db_path, save_path):
    print("Connecting to database...")
    conn = sqlite3.connect(db_path)
    print("Connected!")
    
    print("Fetching conversions data...")
    conversions = pd.read_sql_query("SELECT * FROM conversions", conn)
    print("Conversions data loaded!")
    
    print("Fetching sessions data...")
    sessions = pd.read_sql_query("SELECT * FROM session_sources", conn)
    print("Sessions data loaded!")
    
    # Convert date and time columns into a single datetime column
    conversions['conv_timestamp'] = pd.to_datetime(conversions['conv_date'] + ' ' + conversions['conv_time'])
    sessions['session_timestamp'] = pd.to_datetime(sessions['event_date'] + ' ' + sessions['event_time'])
    
    customer_journeys = []

    print(f"Total conversions to process: {len(conversions)}")
    
    # Process each conversion to find previous sessions
    for idx, conv in conversions.iterrows():
        user_id = conv['user_id']
        conv_id = conv['conv_id']
        conv_time = conv['conv_timestamp']
        
        print(f"Processing conversion {idx + 1}/{len(conversions)} - conv_id: {conv_id}, user_id: {user_id}")
        
        # Get all sessions for the user before the conversion time
        user_sessions = sessions[(sessions['user_id'] == user_id) & (sessions['session_timestamp'] < conv_time)]
        
        print(f"Found {len(user_sessions)} sessions for user {user_id} before conversion {conv_id}")
        
        # Sort sessions by timestamp
        user_sessions = user_sessions.sort_values(by='session_timestamp')

        # Add sessions to the customer journey
        for _, session in user_sessions.iterrows():
            journey_entry = {
                'conversion_id': conv_id,
                'session_id': session['session_id'],
                'timestamp': session['session_timestamp'].strftime('%Y-%m-%d %H:%M:%S'),
                'channel_label': session['channel_name'],  # Assuming 'channel_name' is the correct field
                'holder_engagement': session['holder_engagement'],
                'closer_engagement': session['closer_engagement'],
                'conversion': 0,  # Since the conversion happens after these sessions
                'impression_interaction': session['impression_interaction']
            }
            customer_journeys.append(journey_entry)
    
    print("Processing complete!")
    
    # Save to file
    with open(save_path, "w") as f:
        json.dump(customer_journeys, f, indent=4, default=json_serial)

    print(f"✅ Customer journeys saved to {save_path}")

    conn.close()    
    return customer_journeys

if __name__ == "__main__":
    db_path = "../challenge.db"
    save_path = "customer_journeys.json"

    # Check if journeys are already saved
    if os.path.exists(save_path):
        print(f"Customer journeys already exist in {save_path}, proceeding with execution...")
        with open(save_path, "r") as f:
            journeys = json.load(f)
    else:
        print("Customer journeys not found. Generating journeys first...")
        journeys = get_customer_journeys(db_path, save_path)

    # Check if channel_reporting table exists before sending to the API
    if check_channel_reporting_table_exists(db_path):
        print("Channel Reporting table exists, executing main of channel_reporting_excel.py...")
        channel_reporting_main()  # Execute the main function from channel_reporting_excel.py   
    else:
        print("Channel reporting table does not exist, sending customer data to IHC API...")
        send_to_ihc_api_and_store_results(journeys, db_path, conv_type_id="ihc_challenge")

