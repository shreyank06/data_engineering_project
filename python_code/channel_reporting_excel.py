import sqlite3
import csv
import os
import pandas as pd

def create_channel_reporting_csv(db_path, csv_file_path, start_date=None, end_date=None):
    """
    Creates a .csv file for the channel_reporting table with additional columns:
    - CPO (Cost per Order)
    - ROAS (Return on Ad Spend)
    Filters the data based on the provided start_date and end_date.
    """
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Fetch the data from channel_reporting including the cost, ihc, and ihc_revenue
        query = "SELECT * FROM channel_reporting;"
        rows = pd.read_sql_query(query, conn)

        if not rows.empty:
            print("Data loaded from database.")
        else:
            print("No data found in channel_reporting.")
            return

        # If a date range is provided, filter the rows based on the date
        if start_date and end_date:
            rows['date'] = pd.to_datetime(rows['date'])
            rows = rows[(rows['date'] >= pd.to_datetime(start_date)) & (rows['date'] <= pd.to_datetime(end_date))]

        if rows.empty:
            print(f"No data found for the date range from {start_date} to {end_date}.")
            return

        # Fetch the column names and add the new columns (CPO and ROAS)
        column_names = list(rows.columns) + ["CPO", "ROAS"]

        # Open the CSV file for writing
        with open(csv_file_path, mode='w', newline='') as file:
            writer = csv.writer(file)

            # Write the header (column names)
            writer.writerow(column_names)

            # Write the data with the additional columns
            for _, row in rows.iterrows():
                # Calculate CPO and ROAS
                cost = row['cost']
                ihc = row['ihc']
                ihc_revenue = row['ihc_revenue']

                # Avoid division by zero
                cpo = cost / ihc if ihc != 0 else 0
                roas = ihc_revenue / cost if cost != 0 else 0

                # Write row to CSV including the calculated CPO and ROAS
                writer.writerow(list(row) + [cpo, roas])

        print(f"✅ CSV file created successfully at {csv_file_path}")

    except sqlite3.Error as e:
        print(f"SQLite error occurred: {e}")
    except Exception as e:
        print(f"Error occurred: {e}")
    finally:
        conn.close()

def main():
    db_path = "../challenge.db"  # Adjust path if needed
    csv_file_path = "channel_reporting.csv"  # Adjust file path if needed

    # Ask the user if they want to filter the data by date range
    user_input = input("Do you want to provide a date range for the data? (yes/no): ").strip().lower()
    
    if user_input == 'yes':
        start_date = input("Enter start date (YYYY-MM-DD): ").strip()
        end_date = input("Enter end date (YYYY-MM-DD): ").strip()

        # Create the CSV file with the given date range
        print(f"Creating CSV for the date range: {start_date} to {end_date}")
        create_channel_reporting_csv(db_path, csv_file_path, start_date, end_date)
    else:
        print(f"Creating CSV without data filtering.")
        create_channel_reporting_csv(db_path, csv_file_path)


if __name__ == "__main__":
    main()
