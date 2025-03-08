# Attribution Pipeline Orchestration

## Task

This repository implements a data pipeline that processes data from an **SQLite** database, interacts with the IHC Attribution API, and generates **marketing** **metrics** like Cost per Order (CPO) and Return on Ad Spend (ROAS).
The **pipeline** tracks the **customer** **journey** by tracing all sessions a user interacted with before a **conversion** (e.g., a purchase). It then calculates the impact of each **marketing** **channel** on the conversion using the **IHC** Attribution **API**. The final result shows the **contribution** of each channel to **revenue** (through ROAS) and the **marketing** cost per conversion (**through** CPO).


## Features


1. **Data** **Extraction**: Extracts data from the SQLite database (e.g., session_sources, conversions).
2. **Data** **Transformation**: Transforms data into customer journey formats using Pandas.
3. **API** **Integration**: Sends data in batches to the IHC Attribution API for attribution results.
4. b **Storage**: Writes attribution results to the attribution_customer_journey table and aggregates data into channel_reporting.
5. **CSV** **Export**: Exports the channel_reporting table into a CSV with additional columns for CPO and ROAS.
6. The pipeline is **modular**, easy to maintain, and supports scalable data processing and integration with external APIs.
