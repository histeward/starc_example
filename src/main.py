# Let's build a simple data fetching script 

# Dependencies
import yfinance as yf
import pandas as pd
import matplotlib.pyplot as plt

import os
from google.cloud import bigquery

# Let's extract stock data from apple
stock = "AAPL"

stock_data = yf.Ticker(stock)

# Grab historical stock data
hist = stock_data.history(period="6mo")

# Create DataFrame out of the history data
df = pd.DataFrame(hist)


print("\n##### COLUMNS #####\n")
print(df.columns) # Printing the columns


print("\n##### HEAD #####\n")
print(df.head(5)) # Exploring the data using the head method from pandas


print("\n##### TAIL #####\n")
print(df.tail(5))

# Check for missing values
missing_values = df.isnull().sum()
print("\nMissing values:\n", missing_values)

# Check data types
data_types = df.dtypes
print("\nData types:\n", data_types)

# Check duplicate rows
duplicate_rows = df.duplicated().sum()
print("\nDuplicate rows:", duplicate_rows)


df.columns = [head.lower() for head in df.columns] # list comprehension to lowercase column headers

df.columns = [col.lstrip() if col.startswith(' ') else col for col in df.columns] # remove " " or if columnrow starts with empty space or underscore 

# Visual inspection for outliers
df['close'].plot() # inspecting the Closing price of the data
plt.title("Apple Stock Prices")
plt.ylabel("Price")
plt.xlabel("Date")
plt.show()

# Export DataFrame to CSV locally
try:
    df.to_csv("aaple_stock_hist_6mo_21032024.csv", index=False)
    print("dataframe succesfully exported\n")
except Exception as e:
    print("unsuccessful export: {}".format(str(e)))
    print("Exiting program")
    exit()

# Loads the dataframe to google bigquery, creates a dataset, schema and fills the table
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "./cloud_demo_histeward.json" # credentials for authentication


# initialising BQ client object to communicate with google cloud
client = bigquery.Client()

new_dataset = "aapl_stock_data"  # Name of dataset to create

# List existing datasets
datasets = list(client.list_datasets())  # Make an API request.

if datasets:
    print("Datasets in project {}:".format(client.project))
    for dataset in datasets:
        print("\t{}".format(dataset.dataset_id))

    # Check if the new dataset already exists
    if new_dataset in [dataset.dataset_id for dataset in datasets]:
        print("\nDataset {} already exists.".format(new_dataset))
    else:
        # Create the new dataset
        dataset_id = "{}.{}".format(client.project, new_dataset)
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = "EU"
        try:
            dataset = client.create_dataset(dataset, timeout=30)  # Make an API request.
            print("Created dataset {}.{}".format(client.project, dataset.dataset_id))
        except Exception as e:
            print("Failed to create dataset: {}".format(str(e)))
else:
    print("No datasets found in project {}.".format(client.project))

# load data into bigquery
table_id = "{}.aapl_stock_data.aaple_stock_hist_6mo-20042024".format(client.project)

job_config = bigquery.LoadJobConfig(
    source_format=bigquery.SourceFormat.CSV,
    skip_leading_rows=1,
    autodetect=True,
)

file = "./aaple_stock_hist_6mo_21032024.csv"

with open(file, "rb") as source_file:
    job = client.load_table_from_file(source_file, table_id, job_config=job_config)

job.result()  # Waits for the job to complete.

table = client.get_table(table_id)  # Make an API request.
print(
    "Loaded {} rows and {} columns to {}".format(
        table.num_rows, len(table.schema), table_id
    )
)
