import sqlite3
import pandas as pd
import json
import requests
import time

import os
import dotenv  # Import dotenv

# Load environment variables from .env file
dotenv.load_dotenv()

# Retrieve API key from environment variables
API_KEY = os.getenv("IHC_API_KEY")
#print(API_KEY)

if not API_KEY:
    raise ValueError("API key not found. Set IHC_API_KEY in .env file.")

print("API key loaded successfully!")  # Optional debugging
# Define the API endpoint and authentication (if any)
API_URL = "https://api.ihc-attribution.com/v1/compute_ihc"
#API_KEY = "99c7be20-b723-464e-a0ae-fc81ebe5b12c"  # Replace with your actual API key

# Define the maximum size of chunks to send to the API (based on API limits)
MAX_JOURNEYS_PER_REQUEST = 100  # Maximum 100 customer journeys per request
MAX_SESSIONS_PER_REQUEST = 3000  # Maximum 3000 sessions per request

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
    
    # Close database connection
    conn.close()
    
    return customer_journeys

def send_to_ihc_api_and_store_results(journeys, db_path):
    if not journeys:
        print("No customer journeys found to process.")
        return

    print("Sending customer journeys to IHC API...")

    journey_list = [{"conv_id": conv_id, "sessions": sessions} for conv_id, sessions in journeys.items()]

    customer_journey_batches = [journey_list[i:i + MAX_JOURNEYS_PER_REQUEST]
                                for i in range(0, len(journey_list), MAX_JOURNEYS_PER_REQUEST)]
    
    for batch_idx, batch in enumerate(customer_journey_batches):
        print(f"Processing batch {batch_idx + 1}/{len(customer_journey_batches)} - Total journeys: {len(batch)}")

        data_to_send = []
        for journey in batch:
            conv_id = journey["conv_id"]
            sessions = journey["sessions"]

            session_batches = [sessions[i:i + MAX_SESSIONS_PER_REQUEST]
                               for i in range(0, len(sessions), MAX_SESSIONS_PER_REQUEST)]
            
            for session_batch in session_batches:
                data_to_send.append({
                    "conv_id": conv_id,
                    "sessions": session_batch
                })

        # Ensure conv_type_id is included in the request
        payload = json.dumps({
            "conv_type_id": "your_conversion_type_id",  # Replace with the actual value
            "customer_journeys": data_to_send
        })

        headers = {
            'Content-Type': 'application/json',
            'x-api-key': API_KEY  # Correct header for authentication
        }

        response = requests.post(API_URL, headers=headers, data=payload)

        if response.status_code == 200:
            print(f"Successfully sent batch {batch_idx + 1} of {len(customer_journey_batches)} to IHC API")
            response_data = response.json()
            insert_ihc_results(response_data, db_path)
        else:
            print(f"Error sending batch {batch_idx + 1}: {response.status_code} - {response.text}")

        time.sleep(1)

    print("All customer journeys sent to IHC API!")


def insert_ihc_results(response_data, db_path):
    """
    Inserts the IHC API results into the database.
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    for result in response_data.get("results", []):
        cursor.execute("INSERT INTO ihc_results (conv_id, score) VALUES (?, ?)", 
                       (result["conv_id"], result.get("score", 0)))
    
    conn.commit()
    conn.close()

# Example usage
if __name__ == "__main__":
    db_path = "path_to_your_database.db"  # Adjust path if needed
    journeys = get_customer_journeys(db_path)  # Fetch journeys
    send_to_ihc_api_and_store_results(journeys, db_path)
