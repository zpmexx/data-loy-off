import csv
import os
import pyodbc
import ftplib
from datetime import datetime
from dotenv import load_dotenv

#load env file
load_dotenv()

#download file from ftp
def download_file(ftp_server, ftp_user, ftp_password, remote_filepath, local_filepath):
    try:
        # Connect to the FTP server
        ftp = ftplib.FTP(ftp_server)
        ftp.login(user=ftp_user, passwd=ftp_password)
        print(f"Connected to FTP server: {ftp_server}")

        # Open the local file for writing in binary mode
        with open(local_filepath, 'wb') as local_file:
            # Download the file from the FTP server
            ftp.retrbinary(f"RETR {remote_filepath}", local_file.write)
            print(f"Downloaded {remote_filepath} to {local_filepath}")

        # Close the FTP connection
        ftp.quit()
        print("FTP connection closed")

    except ftplib.all_errors as e:
        print(f"FTP error: {e}")

#remove name and surname wrong chars
def clean_string(s):
    if isinstance(s, str):
        return s.replace(',', '').replace(';', '').replace("'", '').replace('"', '')
    return s

#load env variables
ftp_server = os.getenv('ftp_server')
ftp_user = os.getenv('ftp_user')
ftp_password = os.getenv('ftp_password')
remote_filepath = os.getenv('remote_filepath')
local_filepath = os.getenv('local_filepath')
db_server = os.getenv('db_server')
db_database = os.getenv('db_database')
input_file = os.getenv('input_file')
output_file = os.getenv('output_file')
#download file
download_file(ftp_server, ftp_user, ftp_password, remote_filepath, local_filepath)

print(db_database)
print(db_server)

#open both files  
with open(input_file, mode='r', newline='', encoding='utf-8') as infile, \
     open(output_file, mode='w', newline='', encoding='utf-8') as outfile:
    
    reader = csv.DictReader(infile)
    fieldnames = reader.fieldnames
    
    #read csv
    writer = csv.DictWriter(outfile, fieldnames=fieldnames)
    writer.writeheader()
    
    for row in reader:
        # Remove commas from 'firstname' and 'lastname'
        row['firstname'] = clean_string(row['firstname'])
        row['lastname'] = clean_string(row['lastname'])
        
        # Write the cleaned row to the output file
        writer.writerow(row)
        


#connect to db via windows auth
conn_str = (
    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
    f"SERVER={db_server};"
    f"DATABASE={db_database};"
    f"Trusted_Connection=yes;"
)
#connect to sql server
conn = pyodbc.connect(conn_str)
cursor = conn.cursor()

#sql commands to delete and insert
truncate_sql = 'TRUNCATE TABLE data_loy_off'

insert_sql = """
    INSERT INTO [dbo].[data_loy_off] (
        [id], [custom_identify], [firstname], [lastname], 
        [loy_join_date], [loyalty_level], [off_client_type], 
        [newsletter_agreement], [receive_smses], [city]
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""
data_to_insert = []

#create final file
with open(output_file, mode='r', newline='', encoding='utf-8') as csvfile:
    reader = csv.DictReader(csvfile)
    
    # Insert data into SQL Server
    counter = 1
    for row in reader:
        data_to_insert.append((
            row['id'], row['custom_identify'], row['firstname'], row['lastname'], 
            row['loy_join_date'], row['loyalty_level'], row['off_client_type'], 
            row['newsletter_agreement'], row['receive_smses'], row['city']
        ))

cursor.execute(truncate_sql)

#insert data to sql server
cursor.executemany(insert_sql, data_to_insert)

#save the transaction
conn.commit()

#remeber to close the connection to sql server 
cursor.close()
conn.close()

