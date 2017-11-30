#################################################################################
# This program is designed to build a database containing all US Poastal Codes
# And their accosiated data such as , City, State, Lat and Long.
#################################################################################

import sqlite3
import csv
from datetime import datetime

conn = sqlite3.connect('zip_code_data.db')
c = conn.cursor()
transaction = []


def create_table():
    c.execute("CREATE TABLE IF NOT EXISTS city_data(Zipcode INT PRIMARY KEY, City TEXT, State TEXT, Abbr TEXT, County TEXT,Lattatude REAL, Longitude REAL)")


def sanitize(data):
    data = data.replace("St.", "Saint").replace("Mt.", "Mount").replace("'s", "s").replace("'S", "s").replace("O'", "O-")
    return data


def build_transaction(code, city, state, abbr, county, lat, lonng, row):
    sql = "INSERT INTO city_data(Zipcode, City, State, Abbr, County, Lattatude, Longitude) VALUES ('{}', '{}', '{}', '{}', '{}', '{}', '{}');".format(code, city, state, abbr, county, lat, lonng)
    if row <= 40000:
        sql_transaction_large(sql)
    if row > 40000 and row <= 40900:
        sql_transaction_medium(sql)
    if row > 40900:
        sql_transaction_small(sql)


def sql_transaction_large(sql):
    global transaction
    transaction.append(sql)
    if len(transaction) >= 1000:
        c.execute('BEGIN TRANSACTION')
        for d in transaction:
            c.execute(d)
        conn.commit()
        transaction = []


def sql_transaction_medium(sql):
    global transaction
    transaction.append(sql)
    if len(transaction) >= 100:
        c.execute('BEGIN TRANSACTION')
        for d in transaction:
            c.execute(d)
        conn.commit()
        transaction = []


def sql_transaction_small(sql):
    global transaction
    transaction.append(sql)
    c.execute('BEGIN TRANSACTION')
    for d in transaction:
        c.execute(d)
    conn.commit()
    transaction = []


if __name__ == '__main__':
    create_table()
    row_counter = 0

    with open('us_postal_codes.csv') as file:
        csv_reader = csv.reader(file)
        for row in csv_reader:
            row_counter += 1
            code = row[0]
            city = sanitize(row[1])
            state = row[2]
            abbr = row[3]
            county = sanitize(row[4])
            lat = row[5]
            lonng = row[6]
            build_transaction(code, city, state, abbr, county, lat, lonng, row_counter)

            if row_counter <= 40000:
                if row_counter % 10000 == 0:
                    print("Total Rows Read: {}, Time: {}".format(row_counter, str(datetime.now())))
            if row_counter > 40000 and row_counter <= 40900:
                if row_counter % 100 == 0:
                    print("Total Rows Read: {}, Time: {}".format(row_counter, str(datetime.now())))
            if row_counter > 40900:
                print("Total Rows Read: {}, Time: {}".format(row_counter, str(datetime.now())))
