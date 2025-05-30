# Code for ETL operations on Country-GDP data

# Importing the required libraries
import pandas as pd 
import numpy as np 
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import sqlite3

def log_progress(message):
    ''' This function logs the mentioned message of a given stage of the
    code execution to a log file. Function returns nothing'''
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second 
    now = datetime.now() # get current timestamp 
    timestamp = now.strftime(timestamp_format) 
    with open("./code_log.txt","a") as f: 
        f.write(timestamp + ',' + message + '\n')
        
url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
table_attribs = ['Name','MC_USD_Billion']
def extract(url, table_attribs):
    ''' This function aims to extract the required
    information from the website and save it to a data frame. The
    function returns the data frame for further processing. '''
    response = requests.get(url).text
    soup = BeautifulSoup(response,'html.parser')
    df = pd.DataFrame(columns=table_attribs)
    tables = soup.find_all('tbody')
    rows = tables[0].find_all('tr')
    for row in rows:
        col = row.find_all('td')
        if len(col)!= 0:
            mc_usd_billion = col[2].contents[0].replace('\n','').replace(',','')
            mc_usd_billion = float(mc_usd_billion)
            data_dict = {'Name':col[1].get_text(strip=True),
                         'MC_USD_Billion':mc_usd_billion}
            df1 = pd.DataFrame(data_dict,index=[0])
            df = pd.concat([df,df1], ignore_index = True)
    return df
log_progress('Preliminaries complete. Initiating ETL process')
print("Preliminaries complete. Initiating ETL process")
df = extract(url, table_attribs)
print(df)
log_progress("Data extraction complete. Initiating Transformation process")
print("Data extraction complete. Initiating Transformation process")



csv_path = './exchange_rate.csv'
def transform(df, csv_path):
    ''' This function accesses the CSV file for exchange rate
    information, and adds three columns to the data frame, each
    containing the transformed version of Market Cap column to
    respective currencies'''
    exchange_rate_df = pd.read_csv(csv_path)
    exchange_rate = exchange_rate_df.set_index('Currency').to_dict()['Rate']
    df['MC_GBP_Billion'] = [np.round(x*exchange_rate['GBP'],2) for x in df['MC_USD_Billion']]
    df['MC_EUR_Billion'] = [np.round(x*exchange_rate['EUR'],2) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion'] = [np.round(x*exchange_rate['INR'],2) for x in df['MC_USD_Billion']]
    return df
df = transform(df, csv_path)
pd.set_option('display.max_columns', None)
print(df)
log_progress('Data transformation complete. Initiating loading process')
print('Data transformation complete. Initiating loading process')

output_path = './Largest_banks.csv'
def load_to_csv(df, output_path):
    ''' This function saves the final data frame as a CSV file in
    the provided path. Function returns nothing.'''
    df.to_csv(output_path)
load_to_csv(df,output_path)
log_progress('Data saved to CSV file')
print('Data saved to CSV file')

table_name = 'Largest_banks'
def load_to_db(df, sql_connection,table_name):
    ''' This function saves the final data frame to a database
    table with the provided name. Function returns nothing.'''
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)
sql_connection = sqlite3.connect('Banks.db')
log_progress('SQL connection intiated')
print('SQL connection intiated')
load_to_db(df,sql_connection,table_name)
log_progress('Data loaded to Database as table. Running the query')
print('Data loaded to Database as table. Running the query')


def run_query(query_statement, sql_connection):
    ''' This function runs the query on the database table and
    prints the output on the terminal. Function returns nothing. '''
    print(query_statement)
    query_output = pd.read_sql(query_statement,sql_connection)
    print(query_output)

query_statement = f"SELECT * FROM {table_name}"
run_query(query_statement, sql_connection)
query_statement = f"SELECT AVG(MC_GBP_Billion) FROM {table_name}"
run_query(query_statement, sql_connection)
query_statement = f"SELECT Name from {table_name} LIMIT 5"
log_progress("Process complete")
print("Process complete")

# ''' Here, you define the required entities and call the relevant
# functions in the correct order to complete the project. Note that this
# portion is not inside any function.'''