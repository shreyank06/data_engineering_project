import sqlite3
import csv
import os
import pandas as pd

def create_channel_reporting_csv(db_path, output_dir, filename, start_date=None, end_date=None):
    """
    Creates a .csv file for the channel_reporting table with additional columns:
    - CPO (Cost per Order)
    - ROAS (Return on Ad Spend)
    Filters the data based on the provided start_date and end_date.
    """
    try:
        # Ensure output directory exists
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
            print(f"Created directory: {output_dir}")

        csv_file_path = os.path.join(output_dir, filename)

        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Fetch the data from channel_reporting
        query = "SELECT * FROM channel_reporting;"
        rows = pd.read_sql_query(query, conn)

        if rows.empty:
            print("No data found in channel_reporting.")
            return

        print("Data loaded from database.")

        # If a date range is provided, filter the rows
        if start_date and end_date:
            rows['date'] = pd.to_datetime(rows['date'])
            rows = rows[(rows['date'] >= pd.to_datetime(start_date)) & (rows['date'] <= pd.to_datetime(end_date))]

        if rows.empty:
            print(f"No data found for the date range from {start_date} to {end_date}.")
            return

        # Add new columns (CPO and ROAS)
        rows['CPO'] = rows.apply(lambda row: row['cost'] / row['ihc'] if row['ihc'] != 0 else 0, axis=1)
        rows['ROAS'] = rows.apply(lambda row: row['ihc_revenue'] / row['cost'] if row['cost'] != 0 else 0, axis=1)

        # Save to CSV
        rows.to_csv(csv_file_path, index=False)

        print(f"✅ CSV file created successfully at {csv_file_path}")

    except sqlite3.Error as e:
        print(f"SQLite error occurred: {e}")
    except Exception as e:
        print(f"Error occurred: {e}")
    finally:
        conn.close()

def main():
    db_path = "../challenge.db"  # Adjust path if needed
    output_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "output"))  # One level up
    filename = "channel_reporting.csv"

    # Ask the user if they want to filter the data by date range
    user_input = input("Do you want to provide a date range for the data? (yes/no): ").strip().lower()
    
    if user_input == 'yes':
        start_date = input("Enter start date (YYYY-MM-DD): ").strip()
        end_date = input("Enter end date (YYYY-MM-DD): ").strip()
        print(f"Creating CSV for the date range: {start_date} to {end_date}")
        create_channel_reporting_csv(db_path, output_dir, filename, start_date, end_date)
    else:
        print(f"Creating CSV without data filtering.")
        create_channel_reporting_csv(db_path, output_dir, filename)

if __name__ == "__main__":
    main()
