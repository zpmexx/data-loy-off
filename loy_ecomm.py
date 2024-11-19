import csv
import os
import pyodbc
import ftplib
from datetime import datetime
from dotenv import load_dotenv
import json
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

#load env file
load_dotenv()
#lista błędów
errors = []
now = formatDateTime = None
try:
    now = datetime.now()
    formatDateTime = now.strftime("%d/%m/%Y %H:%M")
except Exception as e:
    errors.append(f'Problem z pobraniem czasu {e}')

#download file from ftp
def download_file(ftp_server, ftp_user, ftp_password, remote_filepath_ecomm, loy_ecomm):
    try:
        # Connect to the FTP server
        ftp = ftplib.FTP(ftp_server)
        ftp.login(user=ftp_user, passwd=ftp_password)
        #print(f"Connected to FTP server: {ftp_server}")

        # Open the local file for writing in binary mode
        with open(loy_ecomm, 'wb') as local_file:
            # Download the file from the FTP server
            ftp.retrbinary(f"RETR {remote_filepath_ecomm}", local_file.write)
           # print(f"Downloaded {remote_filepath} to {loy_ecomm}")

        # Close the FTP connection
        ftp.quit()
        #print("FTP connection closed")

    except ftplib.all_errors as e:
        print(f"FTP error: {e}")

#remove name and surname wrong chars
def clean_string(s):
    if isinstance(s, str):
        return s.replace(',', '').replace(';', '').replace("'", '').replace('"', '')
    return s

#load env variables
try:
    ftp_server = os.getenv('ftp_server')
    ftp_user = os.getenv('ftp_user')
    ftp_password = os.getenv('ftp_password')
    remote_filepath_ecomm = os.getenv('remote_filepath_ecomm')
    loy_ecomm = os.getenv('loy_ecomm')
    db_server = os.getenv('db_server')
    db_database = os.getenv('db_database')
    input_file_ecomm = os.getenv('input_file_ecomm')
    output_file_ecomm = os.getenv('output_file_ecomm')
    from_address = os.getenv('from_address')
    to_address_str = os.getenv('to_address')
    password = os.getenv('password')
except Exception as e:
    errors.append(f'Problem z wczytaniem zmiennych z pliku env {e}')
    
#download file
try:
    download_file(ftp_server, ftp_user, ftp_password, remote_filepath_ecomm, loy_ecomm)
except Exception as e:
    errors.append(f'Problem z pobraniem pliku ecomm z FTP {e}')

#open both files  
with open(input_file_ecomm, mode='r', newline='', encoding='utf-8') as infile, \
     open(output_file_ecomm, mode='w', newline='', encoding='utf-8') as outfile:

    #read csv
    try:
        reader = csv.DictReader(infile)
        fieldnames = reader.fieldnames
    except Exception as e:
        errors.append(f'Problem z odczytaniem pliku wejściowego {e}')
    
    #write new csv
    try:
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()
        
        for row in reader:
            # Remove commas from 'firstname' and 'lastname'
            row['firstname'] = clean_string(row['firstname'])
            row['lastname'] = clean_string(row['lastname'])
            
            # Write the cleaned row to the output file
            writer.writerow(row)
    except Exception as e:
        errors.append(f'Problem z zapisem pliku wyjściowego {e}')
        


#connect to db via windows auth
try:
    conn_str = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={db_server};"
        f"DATABASE={db_database};"
        f"Trusted_Connection=yes;"
    )
    #connect to sql server
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
except Exception as e:
    errors.append(f'Problem z połaczeniem do bazy danych {e}')

#sql commands to delete and insert
truncate_sql = 'TRUNCATE TABLE data_loy_ecomm'

insert_sql = """
    INSERT INTO [dbo].[data_loy_ecomm] (
        [id], [custom_identify], [firstname], [lastname], 
        [loy_join_date], [loyalty_level], [off_client_type], 
        [newsletter_agreement], [receive_smses], [city]
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""
data_to_insert = []

#read from output file
with open(output_file_ecomm, mode='r', newline='', encoding='utf-8') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        data_to_insert.append((
            row['id'], row['custom_identify'], row['firstname'], row['lastname'], 
            row['loy_join_date'], row['loyalty_level'], row['off_client_type'], 
            row['newsletter_agreement'], row['receive_smses'], row['city']
        ))

#clear sql table
try:
    cursor.execute(truncate_sql)

    #insert data to sql server
    cursor.executemany(insert_sql, data_to_insert)

    #save the transaction
    conn.commit()

    #remeber to close the connection to sql server 
    cursor.close()
    conn.close()
except Exception as e:
    errors.append(f'Problem z insertem do bazy danych ecomm {e}')

#wysylanie wiadomosci email
try:
    to_address = json.loads(to_address_str)
    msg = MIMEMultipart()
    msg['From'] = from_address
    msg["To"] = ", ".join(to_address)
    msg['Subject'] = f"Pobieranie piku data_loy_ecomm {formatDateTime}."
    
except Exception as e:
    with open ('logfile.log', 'a') as file:
        file.write(f"""{formatDateTime} Problem z wczytaniem maili\n{str(e)}\n""")
if errors:
    body = "\n".join([str(error) for error in errors])
else:
    body = "Brak błędów. Plik pomyślnie pobrany oraz wgrany do bazy danych."
msg.attach(MIMEText(body, 'html'))
try:
    server = smtplib.SMTP('smtp-mail.outlook.com', 587)
    server.starttls()
    server.login(from_address, password)
    text = msg.as_string()
    server.sendmail(from_address, to_address, text)
    server.quit()    
except Exception as e:
    with open ('logfile.log', 'a') as file:
        file.write(f"""{formatDateTime} Problem z wysłaniem maila\n{str(e)}\n""")