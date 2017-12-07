import sqlite3

conn = sqlite3.connect('zip_code_data.db')
c = conn.cursor()


def get_data(zipcode):
    with conn:
        c.execute("SELECT * FROM city_data WHERE Zipcode='{}'".format(zipcode))
    return c.fetchall()


if __name__ == '__main__':
    end = False
    while end == False:
        print("\nZIPCODE DB QUERY\n--------------------")
        zipcode = input("Please enter a zip code:  ")
        query = get_data(zipcode)
        for item in query:
            print("Zip Code:  ", item[0])
            print("City:  ", item[1])
            print("State:  ", item[2])
            print("Abbreviation:  ", item[3])
            print("County:  ", item[4])
            print("Lattitude:  ", item[5])
            print("Longitude:  ", item[6])
        user = input("\nCheck another entry? (y/n):"  )
        if user == "y":
            end = False
        else:
            print("\nHave a good day!")
            end = True
