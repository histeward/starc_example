# starc_example

### About

This document is just a simple example of fetching data from an api, explore
it, check for necessary anomalies like missing values or outliers, then dumping
it to a CSV file and loading it in a Bigquery data-warehouse. On an enterprise
level if there are a lot of requirements it can take up weeks to build a full
pipeline.

### Project requirements
```md
1. Extract data from yahoo finance

2. Transform the data to be able to comply with google bigquery conventions of storing
    - This can also mean handling missing values
    - Remove duplicates
    - Data type conversion
    - String manipulation
    - Aggregation etc.

3. Dump the data into a CSV file locally

4. Loading the data into bigquery
```

### Workflow

To start off it's best to work in virtual environments. It is to create
isolated environments. It enables you to install dependencies specific to each
project without affecting other projects or the system-wide Python
installation. It ensures consistent and reproducible environments, simplifying
dependency management and making it easier to share and deploy projects.

To enable a virtual env with python you can use the built-in venv package.
```sh
$ python -m venv .venv
```

Activate the virtual environment.
```sh
$ source .venv/bin/activate
```

To keep pip the python package manager up to date use this command.
```sh
$ pip install --upgrade pip
```

After that you are good to go and install dependencies. The dependencies I will use are:

```python
import pandas as pd # Data processing
import yfinance as yf # Abstraction layer to interact with yahoo finance
import matplotlib.pyplot as plt # to plot data and helps visualise outliers
from google.cloud import bigquery # to interact with google bigquery
```

You can install them with:
```sh
$ pip install pandas yfinance matplotlib google-cloud-bigquery
```

Disclamer: The google-cloud-bigquery package is specific to Google's (BAAS)
"back-end as a service" product called Bigquery. This is where I personally
deployed a datawarehouse to store, manipulate or fetch data from for training
or analysis. This was a personal choice. But you can use Snowflake, Azure
synapse, Databricks or other cloud providers to deploy your own datawarehouse or
datalake

### The code

let's import the dependencies first
```python
import pandas as pd
import yfinance as yf
import matplotlib.pyplot as plt
from google.cloud import bigquery
```

Let's fetch some data
```python
# Let's extract stock data from apple from the past 6 months
stock = "AAPL" # initialising the symbol of apple

stock_data = yf.Ticker(stock)

# Grab historical stock data
hist = stock_data.history(period="6mo")
```

Now make a pandas dataframe with the data
```python
# Create DataFrame out of the history data
df = pd.DataFrame(hist)

print("\n##### COLUMNS #####\n")
print(df.columns) # Printing the columns

# Exploring the data using the head/tail method from pandas
print("\n##### HEAD #####\n")
print(df.head(5)) 

print("\n##### TAIL #####\n")
print(df.tail(5))
```

It looks pretty good already. Now we check for anomalies. This will print out the rows of missing values or duplicates
```python
# Check for missing values
missing_values = df.isnull().sum()
print("\nMissing values:\n", missing_values)

# Check data types
data_types = df.dtypes
print("\nData types:\n", data_types)

# Check duplicate rows
duplicate_rows = df.duplicated().sum()
print("\nDuplicate rows:", duplicate_rows)
```

If any of this occurs its good to drop or fill in missing values, drop the
duplicates and/or convert the data types if needed. From here you can transform
the data to your specific requirements.
- divide and group in multiple datasets
- join data with other datasets
- or have multiple sources of data, only extract the data you need and form a new dataset

In this script I kept it simple and just lowercased the headers with a list comprehension as for bigquery's naming convention.
```python
df.columns = [head.lower() for head in df.columns] # list comprehension to lowercase column headers
```

This data does not contain outliers but a good way to check it is to visualise
the data. For fun let's plot the closing price of the data using Matplotlib.
```python
# Visual inspection for outliers
df['close'].plot() # inspecting the Closing price of the data
plt.title("Apple Stock Prices")
plt.ylabel("Price")
plt.xlabel("Date")
plt.show()
```

Now using to_csv() method from python I can dump the data into a csv file
```python
# Export DataFrame to CSV locally
try:
    df.to_csv("aaple_stock_hist_6mo_21032024.csv", index=False)
    print("dataframe succesfully exported\n")
except Exception as e:
    print("unsuccessful export: {}".format(str(e)))
    print("Exiting program")
    exit()
```

For the final part we need to communicate with __google cloud bigquery__, create
a dataset and a schema/table. But in Google's terminology, __bigquery
datasets are containers for tables, views, or ML models.__

Alright Time to communicate with google cloud and fetch information. For
authentication I need the os library to get environment variables.

Import the `os` module, which provides a portable way of using operating
system-dependent functionality.

```python
import os
```

Use the `os.environ` to set the `GOOGLE_APPLICATION_CREDENTIALS` environment
variable to the path of your JSON file containing the authentication key.
Replace `'./json_file_with_the_aut_key.json'` with the path to your JSON file.

```python
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = './json_file_with_the_aut_key.json'
```

After the authentication we need to initialise a client object to communicate with google cloud
```python
# initialising BQ client object to communicate with google cloud
client = bigquery.Client()
```

Now for logic for creating a dataset
```python

new_dataset = "aapl_stock_data"  # Name of dataset to create

# List existing datasets
datasets = list(client.list_datasets())  # Make an API request to get datasets

if datasets:
    print("Datasets in project {}:".format(client.project))
    for dataset in datasets:
        print("\t{}".format(dataset.dataset_id)) # printing existing datasets

    # Check if the new dataset already exists
    if new_dataset in [dataset.dataset_id for dataset in datasets]:
        print("\nDataset {} already exists.".format(new_dataset))
    else:
        # Create the new dataset
        dataset_id = "{}.{}".format(client.project, new_dataset)
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = "EU" # Location of which servers from google the dataset will be stored
        try:
            dataset = client.create_dataset(dataset, timeout=30)  # Make an API request to create dataset
            print("Created dataset {}.{}".format(client.project, dataset.dataset_id))
        except Exception as e:
            print("Failed to create dataset: {}".format(str(e)))
else:
    print("No datasets found in project {}.".format(client.project))
```

And to finalise this, we are going to load the csv file that we exported to
google bigquery by creating a new table inside the cloud and dumping the data
into it.
```python
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

job.result()  # Waits for the job to complete loading the data in google cloud

table = client.get_table(table_id)  # Make an API request to fetch information
print(
    "Loaded {} rows and {} columns to {}".format(
        table.num_rows, len(table.schema), table_id
    )
)
```

__Notes:__
- a quirk is that not all technologies are compatible
- which is pandas method of constructing a dataframe and google cloud reading the pandas dataframe structure
- I had to export the pandas dataframe locally to a csv file
- then used python again to read the csv file in binary instead of text
- then the api call could accept it and dump the data in google cloud
