import pyodbc
import pandas as pd

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
    cnxn.close()
    print ("Connection closed")
except:
    print("Error! Something else went wrong")
    cnxn.close()
    print ("Connection closed")
else:
    print ("Connection established")

cursor = cnxn.cursor()

#Check if the table is exist
check_querry = '''
                IF NOT EXISTS
                    (
                        SELECT * FROM sys.objects
                        WHERE object_id = OBJECT_ID(N'dbo.DATE_DIM_STAGING')
                    )
	                BEGIN
		                SELECT 0
	                END
                ELSE
	                BEGIN
		                SELECT 1
	                END
                '''
cursor.execute(check_querry)
check_result = cursor.fetchone() 
if check_result[0] == 0:
    print ('Error! DATE_DIM_STAGING table does not exist')
    cnxn.close()
    print ("Connection closed")
    exit()
else:
    print ('DATE_DIM_STAGING table exists')

#Get the location of the data file from the config file
print("Try to find the config file.....")
try:
    with open('Data_File_config.cfg') as f:
        data_file = f.read()
except FileNotFoundError:
    print ("Error! The config file is not found")
    cnxn.close()
    print ("Connection closed")
    exit()
except:
    print("Error! Something else went wrong")
    cnxn.close()
    print ("Connection closed")
    exit()
else:
    print ("The data file is found")

#Try to get the data
print("Getting the data .....")
try:
    #Import the CSV file into Python using the pandas library
    data = pd.read_csv(data_file)
except:
    print("Error! Something went wrong! Check the data file!")
    cnxn.close()
    print ("Connection closed")
    exit()
else:
    print ("The data is read")

#Create the DataFrame using the pandas library
df = pd.DataFrame(data)

#Put the data to the staging table
try:
    for row in df.itertuples():
        cursor.execute('''
                        INSERT INTO DDDB.dbo.DATE_DIM_STAGING (
                            DateNum,
                            Date,
                            YearMonthNum,
                            Calendar_Quarter,
                            MonthNum,
                            MonthName,
                            MonthShortName,
                            WeekNum,
                            DayNumOfYear,
                            DayNumOfMonth,
                            DayNumOfWeek,
                            DayName,
                            DayShortName,
                            Quarter,
                            YearQuarterNum,
                            DayNumOfQuarter
                        )
                        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                        ''',
                        row.DateNum,
                        row.Date,
                        row.YearMonthNum,
                        row.Calendar_Quarter,
                        row.MonthNum,
                        row.MonthName,
                        row.MonthShortName,
                        row.WeekNum,
                        row.DayNumOfYear,
                        row.DayNumOfMonth,
                        row.DayNumOfWeek,
                        row.DayName,
                        row.DayShortName,
                        row.Quarter,
                        row.YearQuarterNum,
                        row.DayNumOfQuarter
                        )
    cnxn.commit()   
except:
    print ("SQL query error! (Put the data to the staging table)")
    cnxn.close()
    print ("Connection closed")
    exit()
else:
    print ("The data loaded into the staging table successfully!")

#Perform ETL procedures
try:
#Let's calculate the count of columns
    ETL_querry = '''
                    SELECT COUNT(*)
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE table_catalog = 'DDDB'
                    AND table_name = 'DATE_DIM_STAGING'
                 '''
    cursor.execute(ETL_querry)
    result = cursor.fetchone() 
    print ("The count of columns: ", result[0])
except:
    print ("SQL query error! (Perform ETL procedures)")
    cnxn.close()
    print ("Connection closed")
    exit()
else:
    print ("ETL procedures executed successfully!")

#Transfer the data to the main table
try:
    cursor.execute(
        '''
        SELECT *
        INTO DATE_DIM
        FROM DATE_DIM_STAGING;
        '''
    )
    cnxn.commit()
except:
    print ("SQL query error! (Transfer the data to the main table)")
    cnxn.close()
    print ("Connection closed")
    exit()
else:
    print ("The data transferred into the main table successfully!")
finally:
    cnxn.close()
    print ("Connection closed")