
# OpenFIGI Tickers

OpenFIGI Tickers is a Python-based ETL pipeline that integrates with the OpenFIGI API to normalize and enrich ticker symbols from multiple data sources. Designed for deployment in financial data environments, the system loads raw instrument data from an MSSQL database, enriches it with FIGI identifiers, and stores the transformed results into a configured output table.

## Overview

This tool streamlines mapping and transformation of financial instrument identifiers using FIGI (Financial Instrument Global Identifier). It leverages SQL-based input queries and multithreaded batch processing to scale efficiently.

## Application Flow

### Inputs

The system requires the following input data and configuration:

- **Environment variables (.env)**: These control logging, MSSQL access, OpenFIGI API credentials, and SQL queries.
- **SQL Queries (_QUERY variables)**: Environment variables that define SQL queries for loading data from MSSQL. These include:
  - `ISHARES_QUERY`
  - `EOD_TICKERS_QUERY`
  - `CURRENCIES_QUERY`
  - `ALL_EXCHANGES_QUERY`
  - `EXCHANGES_PRIORITY_QUERY`
- **BrightData Proxy**: Used for routing outbound OpenFIGI API requests.

### Process Steps

1. **Database Data Load**:
   - Executes the SQL queries defined in environment variables.
   - Loads datasets into in-memory Pandas DataFrames.

2. **Data Preparation**:
   - Maps ISINs and tickers to standardized formats.
   - Structures OpenFIGI query payloads based on issuer and exchange mappings.

3. **OpenFIGI API Integration**:
   - Sends batch requests to the OpenFIGI API.
   - Utilizes multithreading to parallelize lookup operations.
   - Retries failed requests with exponential backoff.

4. **Post-Processing**:
   - Combines initial and secondary FIGI responses.
   - Filters duplicate or ambiguous results.
   - Resolves final identifiers based on exchange and priority logic.

5. **Output**:
   - Generates a clean, enriched dataset.
   - Saves to CSV (`openfigi_transformed.csv`).
   - Inserts into a specified MSSQL table (configured via `OUTPUT_TABLE`).

## Installation

```bash
git clone https://github.com/pvotio/openfigi-tickers.git
cd openfigi-tickers
cp .env.sample .env  # and configure it
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## Running the Pipeline

```bash
python main.py
```

## Docker

```bash
docker build -t openfigi-tickers .
docker run --env-file .env openfigi-tickers
```

## License

This project is under the MIT License.
