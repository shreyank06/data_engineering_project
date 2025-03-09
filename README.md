# Attribution Pipeline Orchestration

## Task

This repository implements a data pipeline that processes data from an **SQLite** database, interacts with the IHC Attribution API, and generates **marketing** **metrics** like Cost per Order (CPO) and Return on Ad Spend (ROAS).
The **pipeline** tracks the **customer** **journey** by tracing all sessions a user interacted with before a **conversion** (e.g., a purchase). It then calculates the impact of each **marketing** **channel** on the conversion using the **IHC** Attribution **API**. The final result shows the **contribution** of each channel to **revenue** (through ROAS) and the **marketing** cost per conversion (**through** CPO).

## General Flow of the Pipeline:

1. Run **customer_journey**.**py** → Checks if journeys are generated. If not, creates them.
2. Check if **channel_reporting** exists:
3. If yes: Executes aggregation and reporting.
4. If no: Sends **data** to the **IHC** **API** for processing.
5. **Generate** **CSV** **Report** → The pipeline can create an exportable **CSV** file with additional calculated fields like **CPO** (**Cost** **per** **Order**) and **ROAS** (Return on Ad Spend).
   
## Steps to run pipeline
### Install Dependencies
Ensure all the necessary dependencies are installed, including libraries like sqlite3, pandas, requests, dotenv, and others required in the scripts. You can install them via pip:
```
pip install pandas requests python-dotenv
```
### Set Up Environment Variables
Before running the scripts, ensure you have the necessary environment variables set up in a .env file for the API key, which is required by the send_to_ihc_api.py script. 
```
IHC_API_KEY=your_api_key_here
```
###  Run customer_journey.py
Start by running customer_journey.py to generate customer journeys from the database and save them to customer_journeys.json. This script checks if the customer journeys already exist before proceeding.
```
python3 customer_journey.py
```
Behavior:
If customer_journeys.json exists, it skips the generation and loads the data.
If the channel_reporting table exists in the database, it runs channel_reporting_excel.py.
If not, it calls the send_to_ihc_api.py script to send customer journeys to the API.



