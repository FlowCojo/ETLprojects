from bs4 import BeautifulSoup
import requests
import pandas as pd
import numpy as np
import sqlite3
from datetime import datetime 

def log_progress(message):
    ''' This function logs the mentioned message at a given stage of the 
    code execution to a log file'''

    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second 
    now = datetime.now() # get current timestamp 
    timestamp = now.strftime(timestamp_format) 
    with open("./code_log.txt","a") as f: 
        f.write(timestamp + ' : ' + message + '\n')    

def extract(url, table_attribs):
    ''' The purpose of this function is to extract the required
    information from the website and save it to a dataframe. The
    function returns the dataframe for further processing. '''

    page = requests.get(url).text
    data = BeautifulSoup(page,'html.parser')
    df = pd.DataFrame(columns=table_attribs)
    tables = data.find_all('tbody')
    rows = tables[0].find_all('tr')
    for row in rows:
        col = row.find_all('td')
        if len(col) > 2:  # Ensure that the row has the correct number of columns
            # Extract the bank name from the <a> tag or directly from the <td>
            bank_name = col[1].find_all('a')[1]['title']
            mc_usd_billion=float(col[2].contents[0][:-1])

            data_dict = {"Name": bank_name, "MC_USD_Billion": mc_usd_billion}
            df1 = pd.DataFrame(data_dict, index=[0])
            df = pd.concat([df, df1], ignore_index=True)

    return df

def transform(df, csv_path):
    df_rate=pd.read_csv(csv_path)
    exchange_rate = df_rate.set_index('Currency').to_dict()['Rate']
    df['MC_EUR_Billion'] = [np.round(x*exchange_rate['EUR'],2) for x in df['MC_USD_Billion']]
    df['MC_GBP_Billion'] = [np.round(x*exchange_rate['GBP'],2) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion'] = [np.round(x*exchange_rate['INR'],2) for x in df['MC_USD_Billion']]

    return df


def load_to_csv(df, csv_path):
    df.to_csv(csv_path)

def load_to_db(df, sql_connection, table_name):
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)

def run_query(query_statement, sql_connection):
    ''' This function runs the stated query on the database table and
    prints the output on the terminal'''

    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)

url="https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks"
table_attribs = ["Name", "MC_USD_Billion"]
table_name = 'Largest_banks'
output_csv_path = './Largest_banks_data.csv'
rate_csv_path='./exchange_rate.csv'
db_name='Banks.db'


log_progress('Preliminaries complete. Initiating ETL process')

df = extract(url, table_attribs)
# print(df)
log_progress('Data extraction complete. Initiating Transformation process')

pd.set_option('display.max_colwidth', 1000)
df = transform(df,rate_csv_path)
print(df)
log_progress('Data transformation complete. Initiating loading process')

load_to_csv(df, output_csv_path)

log_progress('Data saved to CSV file')

sql_connection = sqlite3.connect('Banks.db')

log_progress('SQL Connection initiated.')

load_to_db(df, sql_connection, table_name)

log_progress('Data loaded to Database as table. Running the query 1')

query_statement1 = f"SELECT * from {table_name} "
run_query(query_statement1, sql_connection)

log_progress('Data loaded to Database as table. Running the query 2')

query_statement2 = f"SELECT AVG(MC_USD_Billion),AVG(MC_EUR_Billion),AVG(MC_GBP_Billion),AVG(MC_INR_Billion) FROM {table_name} "
run_query(query_statement2, sql_connection)

log_progress('Data loaded to Database as table. Running the query 3')

query_statement3 = f"SELECT Name from {table_name} LIMIT 5 "
run_query(query_statement3, sql_connection)

log_progress('Process Complete.')

sql_connection.close()