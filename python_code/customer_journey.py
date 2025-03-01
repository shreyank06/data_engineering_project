import sqlite3
import pandas as pd
from send_to_ihc_api import send_to_ihc_api_and_store_results  # Importing the function from send_to_ihc.py
import os
import json

def json_serial(obj):
    """Custom JSON serializer for objects not serializable by default."""
    if isinstance(obj, pd.Timestamp):
        return obj.isoformat()  # Convert Timestamp to ISO 8601 string format
    raise TypeError("Type not serializable")

def get_customer_journeys(db_path):
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
    
    customer_journeys = {}
    
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
        
        # Store the journey
        customer_journeys[conv_id] = user_sessions.to_dict(orient='records')
    
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
        journeys = get_customer_journeys(db_path)

    # Execute the next module after ensuring journeys are available
    send_to_ihc_api_and_store_results(journeys, db_path)
