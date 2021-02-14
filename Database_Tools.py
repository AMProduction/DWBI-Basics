import pyodbc

#Read the DB credential from the config file
print("Reading the database credential.....")
try:
    with open('DB_config.cfg') as c:
        credential = c.read()
except FileNotFoundError:
    print ("Error! File \'DB_config.cfg\' is not found")
    exit()
except:
    print("Error! Something else went wrong")
    exit()
else:
    print ("Credential is read")

cnxn_str = (credential)

#Connect to the DB
try:
    cnxn = pyodbc.connect(cnxn_str)
except ConnectionError:
    print ("Connection error")
    exit()
except:
    print("Error! Something else went wrong")
    exit()
else:
    print ("Connection established")

cursor = cnxn.cursor()

#Read the SQL-query create a staging table from the config file
try:
    with open('CREATE_TABLE.sql') as f:
        querry = f.read()
except FileNotFoundError:
    print ("Error! File \'CREATE_TABLE.sql\' is not found")
    cnxn.close()
    print ("Connection closed")
    exit()
except:
    print("Error! Something else went wrong")
    cnxn.close()
    print ("Connection closed")
    exit()
else:
    print ("Query is read")

#Execute the SQL-query create the staging table
try:
    cursor.execute(querry)
    cnxn.commit()
except:
    print ("SQL query error!")
else:
    print ("The staging table DATE_DIM is created successfully!")
finally:
    cnxn.close()
    print ("Connection closed")