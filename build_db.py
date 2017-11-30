#################################################################################
# This program is designed to build a database containing all US Poastal Codes
# And their accosiated data such as , City, State, Lat and Long.
#################################################################################

import sqlite3
import csv
from datetime import datetime

# Delcare sqlite3 connection , cursor and database name
conn = sqlite3.connect('zip_code_data.db')
c = conn.cursor()
# Transaction list is used for the sqlite transactions, using this as global
# was the only way I could get proper operation from the transaction
# Possibly a better way?
transaction = []


# Create the "city_data" table in if not already present
def create_table():
    c.execute("CREATE TABLE IF NOT EXISTS city_data(Zipcode INT PRIMARY KEY, City TEXT, State TEXT, Abbr TEXT, County TEXT,Lattatude REAL, Longitude REAL)")

# Cleaning up the data of the CSV to prevent errors when populating the DB
def sanitize(data):
    data = data.replace("St.", "Saint").replace("Mt.", "Mount").replace("'s", "s").replace("'S", "s").replace("O'", "O-")
    return data

# Building the list item before passing to the appropriate transacation
def build_transaction(code, city, state, abbr, county, lat, lonng, row):
    sql = "INSERT INTO city_data(Zipcode, City, State, Abbr, County, Lattatude, Longitude) VALUES ('{}', '{}', '{}', '{}', '{}', '{}', '{}');".format(code, city, state, abbr, county, lat, lonng)
    # To optimize INSERTS, performed multiple types of transactions
    # CSV contains 40933 rows so multiple transactions were needed
    if row <= 40000:
        sql_transaction_large(sql)
    if row > 40000 and row <= 40900:
        sql_transaction_medium(sql)
    if row > 40900:
        sql_transaction_small(sql)


# Appending the passed INSERT into the transaction list
# Once the list contains 1000 elements then the transaction is executed
# This significanlty reduced the transaction time
def sql_transaction_large(sql):
    global transaction
    transaction.append(sql)
    if len(transaction) >= 1000:
        c.execute('BEGIN TRANSACTION')
        for d in transaction:
            c.execute(d)
        conn.commit()
        transaction = []

# Appending the passed INSERT into the transaction list
# Once the list contains 100 elements then the transaction is executed
def sql_transaction_medium(sql):
    global transaction
    transaction.append(sql)
    if len(transaction) >= 100:
        c.execute('BEGIN TRANSACTION')
        for d in transaction:
            c.execute(d)
        conn.commit()
        transaction = []

# Transaction for the last 33 entries in the CSV
# Appending the passed INSERT into the transaction list
# The transaction list contains only one INSERT before it is excecuted
def sql_transaction_small(sql):
    global transaction
    transaction.append(sql)
    c.execute('BEGIN TRANSACTION')
    for d in transaction:
        c.execute(d)
    conn.commit()
    transaction = []


if __name__ == '__main__':
    # NULL if table already exists
    create_table()
    row_counter = 0

    # open the CSV, iterate over each row and insert each line into the DB

    with open('us_postal_codes.csv') as file:
        csv_reader = csv.reader(file)
        for row in csv_reader:
            row_counter += 1
            code = row[0]
            # Cleaning up items to prevent errors
            city = sanitize(row[1])
            state = row[2]
            # State abreviation
            abbr = row[3]
            # Cleaning up rows to prevent errors
            county = sanitize(row[4])
            lat = row[5]
            lonng = row[6]
            build_transaction(code, city, state, abbr, county, lat, lonng, row_counter)

            # Prints to the console to inform user of rows read
            if row_counter <= 40000:
                if row_counter % 10000 == 0:
                    print("Total Rows Read: {}, Time: {}".format(row_counter, str(datetime.now())))
            if row_counter > 40000 and row_counter <= 40900:
                if row_counter % 100 == 0:
                    print("Total Rows Read: {}, Time: {}".format(row_counter, str(datetime.now())))
            if row_counter > 40900:
                print("Total Rows Read: {}, Time: {}".format(row_counter, str(datetime.now())))
