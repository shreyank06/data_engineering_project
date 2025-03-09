import json
import requests
import time
import os
import dotenv
from attribution_customer_journey import create_attribution_customer_journey_table, clear_attribution_customer_journey_table, insert_ihc_results

# Load environment variables from .env file
dotenv.load_dotenv()

# Retrieve API key from environment variables
API_KEY = os.getenv("IHC_API_KEY")

if not API_KEY:
    raise ValueError("API key not found. Set IHC_API_KEY in .env file.")

print("API key loaded successfully!")  # Optional debugging

# Define the API endpoint
API_URL = "https://api.ihc-attribution.com/v1/compute_ihc"

# Define the maximum size of chunks to send to the API (based on API limits)
MAX_JOURNEYS_PER_REQUEST = 100  # Maximum 100 customer journeys per request
MAX_SESSIONS_PER_REQUEST = 2000  # Maximum 200 sessions per request (to comply with free-tier limit)

def send_to_ihc_api_and_store_results(journeys, db_path, conv_type_id):
    # Create table if it doesn't exist
    create_attribution_customer_journey_table(db_path)

    # Clear the table before inserting new data
    clear_attribution_customer_journey_table(db_path)

    if not journeys:
        print("No customer journeys found to process.")
        return

    print(f"Sending {len(journeys)} customer journeys to IHC API...")

    # Prepare the data in the required format
    data_to_send = []

    for journey in journeys:
        # Removing any unnecessary fields (e.g., 'impression_interaction') from the journey
        data_to_send.append(journey)

    # Ensure the body is a list of dictionaries
    if not isinstance(data_to_send, list) or not all(isinstance(item, dict) for item in data_to_send):
        print("Error: Data must be a list of dictionaries.")
        return

    api_url = f"https://api.ihc-attribution.com/v1/compute_ihc?conv_type_id={conv_type_id}"

    # Send requests in batches
    customer_journey_batches = [data_to_send[i:i + MAX_JOURNEYS_PER_REQUEST]
                                for i in range(0, len(data_to_send), MAX_JOURNEYS_PER_REQUEST)]

    for batch_idx, batch in enumerate(customer_journey_batches):
        print(f"Processing batch {batch_idx + 1}/{len(customer_journey_batches)} - Total journeys: {len(batch)}")

        # Body with customer journeys
        body = {
            'customer_journeys': batch
        }

        headers = {
            'Content-Type': 'application/json',
            'x-api-key': API_KEY
        }

        # Send the request
        try:
            response = requests.post(api_url, data=json.dumps(body), headers=headers)
            response.raise_for_status()  # Raise an exception for HTTP errors

            if response.status_code == 200:
                print(f"Successfully sent batch {batch_idx + 1} of {len(customer_journey_batches)} to IHC API")
                response_data = response.json()
                insert_ihc_results(response_data, db_path)
            else:
                print(f"Error sending data: {response.status_code} - {response.text}")
        except requests.exceptions.RequestException as e:
            print(f"Request failed: {e}")
        
        # Optional: Add delay to avoid rate-limiting
        #time.sleep(5)  

    print("All customer journeys sent to IHC API!")

# Example usage
if __name__ == "__main__":
    db_path = "../challenge.db"  # Adjust path if needed
    conv_type_id = 'ihc_challenge'  # Insert your conversion type ID here

    # Assuming you fetch the customer journeys somehow (not defined in this code)
    journeys = []  # Put your actual method to fetch customer journeys here

    if journeys:
        send_to_ihc_api_and_store_results(journeys, db_path, conv_type_id)
    else:
        print("No journeys to process.")
