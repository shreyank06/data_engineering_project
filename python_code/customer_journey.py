import sqlite3
import pandas as pd
from send_to_ihc_api import send_to_ihc_api_and_store_results  # Importing the function from send_to_ihc.py
import os
import json
from channel_reporting_excel import main as channel_reporting_main
from channel_reporting_table import populate_channel_reporting, check_ihc_sum_condition

def json_serial(obj):
    """Custom JSON serializer for objects not serializable by default."""
    if isinstance(obj, pd.Timestamp):
        return obj.isoformat()  # Convert Timestamp to ISO 8601 string format
    raise TypeError("Type not serializable")

def check_table_exists(db_path, table_name):
    """
    Check if a table exists in the database.
    """
    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?;", (table_name,))
            return cursor.fetchone() is not None
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
    
    conversions['conv_timestamp'] = pd.to_datetime(conversions['conv_date'] + ' ' + conversions['conv_time'])
    sessions['session_timestamp'] = pd.to_datetime(sessions['event_date'] + ' ' + sessions['event_time'])
    
    customer_journeys = []

    print(f"Total conversions to process: {len(conversions)}")
    
    for idx, conv in conversions.iterrows():
        user_id = conv['user_id']
        conv_id = conv['conv_id']
        conv_time = conv['conv_timestamp']
        
        print(f"Processing conversion {idx + 1}/{len(conversions)} - conv_id: {conv_id}, user_id: {user_id}")
        
        user_sessions = sessions[(sessions['user_id'] == user_id) & (sessions['session_timestamp'] < conv_time)]
        
        print(f"Found {len(user_sessions)} sessions for user {user_id} before conversion {conv_id}")
        
        user_sessions = user_sessions.sort_values(by='session_timestamp')

        for _, session in user_sessions.iterrows():
            journey_entry = {
                'conversion_id': conv_id,
                'session_id': session['session_id'],
                'timestamp': session['session_timestamp'].strftime('%Y-%m-%d %H:%M:%S'),
                'channel_label': session['channel_name'],
                'holder_engagement': session['holder_engagement'],
                'closer_engagement': session['closer_engagement'],
                'conversion': 0,
                'impression_interaction': session['impression_interaction']
            }
            customer_journeys.append(journey_entry)
    
    print("Processing complete!")
    
    with open(save_path, "w") as f:
        json.dump(customer_journeys, f, indent=4, default=json_serial)

    print(f"✅ Customer journeys saved to {save_path}")
    
    conn.close()    
    return customer_journeys

if __name__ == "__main__":
    db_path = "../challenge.db"
    save_path = "customer_journeys.json"

    if os.path.exists(save_path):
        print(f"Customer journeys already exist in {save_path}, proceeding with next steps...")
        with open(save_path, "r") as f:
            journeys = json.load(f)
        
        if check_table_exists(db_path, 'channel_reporting'):
            print("Channel Reporting table exists, proceeding with CSV file generation...")
            check_ihc_sum_condition(db_path)
            user_input = input("Do you want to prepare the CSV file? (yes/no): ").strip().lower()
            
            if user_input == 'yes':
                print("📝 Continuing with preparing the CSV file...")
                channel_reporting_main()
            else:
                print("❌ CSV preparation skipped.")
        else:
            if not check_table_exists(db_path, 'attribution_customer_journey'):
                print("Attribution customer journey table does not exist, sending customer data to IHC API...")
                send_to_ihc_api_and_store_results(journeys, db_path, conv_type_id="ihc_challenge")
                populate_channel_reporting(db_path)
            else:
                print("Attribution customer journey table exists, populating channel reporting table...")
                populate_channel_reporting(db_path)
                check_ihc_sum_condition(db_path)
                channel_reporting_main()
    else:
        print("Customer journeys not found. Generating journeys first...")
        journeys = get_customer_journeys(db_path, save_path)
