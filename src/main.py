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

[head.lower() for head in df.columns] # list comprehension to lowercase column headers

df.columns = [head.lower() for head in df.columns] # list comprehension to lowercase column headers

# df.columns = [col.lstrip() if col.startswith(' ') else col for col in df.columns] # remove " " or if columnrow starts with empty space or underscore 

print("\n##### HEAD #####\n")
print(df.head(5)) # Exploring the data using the head method from pandas


print("\n##### TAIL #####\n")
print(df.tail(5))

# Check for missing values
missing_values = df.isnull().sum()
print("Missing values:\n", missing_values)


# Check data types
data_types = df.dtypes
print("Data types:\n", data_types)

# Check duplicate rows
duplicate_rows = df.duplicated().sum()
print("duplicate rows:", duplicate_rows)

# Visual inspection for outliers
df['close'].plot() # inspecting the Closing price of the data
plt.title("Apple Stock Prices")
plt.ylabel("Price")
plt.xlabel("Date")
plt.show()

# Export DataFrame to CSV locally
try:
    df.to_csv("aaple_stock_hist_6mo_20042024.csv", index=False)
    print("dataframe succesfully exported\n")
except:
    print("dataframe unsuccesfully exported\n")
    print("Exiting program")
    exit()

# Loads the dataframe to google bigquery, creates a dataset, schema and fills the table

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = './json_file_with_the_aut_key.json' # credentials for authentication

# initialising BG client
client = bigquery.Client()

# create dataset_id to the ID of the dataset to create.
dataset_id = "{}.aapl_stock_data".format(client.project)

# Construct a full Dataset object to send to the API.
dataset = bigquery.Dataset(dataset_id)

# specify the geographic location where the dataset should reside.
dataset.location = "EU"

# Send the dataset to the API for creation
try:
    dataset = client.create_dataset(dataset, timeout=30)  # Make an API request.
    print("Created dataset {}.{}".format(client.project, dataset.dataset_id))
except:
    print("dataset exists already")

table_id = "{}.aapl_stock_data.aaple_stock_hist_6mo-20042024".format(client.project)

job_config = bigquery.LoadJobConfig(
    source_format=bigquery.SourceFormat.CSV,
    skip_leading_rows=1,
    autodetect=True,
)
file = "./aaple_stock_hist_6mo_20042024.csv"

with open(file, "rb") as source_file:
    job = client.load_table_from_file(source_file, table_id, job_config=job_config)

job.result()  # Waits for the job to complete.

table = client.get_table(table_id)  # Make an API request.
print(
    "Loaded {} rows and {} columns to {}".format(
        table.num_rows, len(table.schema), table_id
    )
)
