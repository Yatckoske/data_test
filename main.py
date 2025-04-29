import csv
from sqlalchemy import create_engine, MetaData, Table, Column, String

# Database configuration
db_username = "admin"
db_password = "COFK"
db_name = "cofk_staging"
db_host = "10.100.0.3"
db_port = "5432"

# Create an SQLAlchemy engine to connect to the database
engine = create_engine(f'postgresql://{db_username}:{db_password}@{db_host}:{db_port}/{db_name}')

# Create a metadata instance
metadata = MetaData()

# Open and read the CSV file to count the total number of rows
csv_file_path = r"C:\Users\AdrickYatckoske\Downloads\ServiceLineDetailReport.csv"
with open(csv_file_path, 'r') as csvfile:
csvreader = csv.reader(csvfile)
total_rows = sum(1 for row in csvreader) - 1  # Subtract 1 to exclude the header row

# Open and read the CSV file again to insert data into the database
with open(csv_file_path, 'r') as csvfile:
    csvreader = csv.reader(csvfile)
    headers = next(csvreader)  # Get the headers from the first row of the CSV

    # Define the table structure based on the headers
    data_table = Table('SLDR_2024', metadata,
                       *[Column(header, String) for header in headers])

    # Create the table
    metadata.create_all(engine)

    # Prepare a list to store data for batch insert
    data_buffer = []
    inserted_rows = 0

    for row in csvreader:
        data_buffer.append({header: value for header, value in zip(headers, row)})
        inserted_rows += 1

        # Check if batch size reached or all rows processed
if len(data_buffer) == 10000 or inserted_rows == total_rows:
            # Insert data in batch
            engine.execute(data_table.insert(), data_buffer)
            data_buffer = []  # Clear buffer for next batch

    # Close the engine connection
engine.dispose()

# Print a message once all data has been loaded
print("Data loaded successfully.")
