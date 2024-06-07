import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import sys
from io import StringIO
import datetime

# Opens the config.py file that contains the information to access the database. Throws errors otherwise
def importConfig():
    try:
        from config import config_db
    except ImportError:
        print('''
            Verify that the config.py file is in the same directory as the load_data.py
            Verify that the dictionary is config_db is named correctly in the config.py
            ''')
        sys.exit('Please verify all these changes and try again.')
    return config_db

'''
Creates 'journeys' and 'stations' tables if they don't exist. 
Outputs any errors that may arise.
'''
def createTables(cur, conn):
    createStationTableQuery = '''CREATE TABLE IF NOT EXISTS stations
                                (station_pk         SERIAL PRIMARY KEY NOT NULL,
                                station_id          TEXT UNIQUE NOT NULL DEFAULT NULL,
                                station_name        TEXT,
                                capacity            TEXT,
                                latitude            TEXT,
                                longitude           TEXT
                                )
                            '''
    createJourneyTableQuery = '''CREATE TABLE IF NOT EXISTS journeys
                                (journey_pk         SERIAL PRIMARY KEY NOT NULL,
                                journey_id          TEXT NOT NULL DEFAULT NULL,
                                journey_duration    TEXT, 
                                start_station_id    TEXT REFERENCES stations(station_id) ON DELETE CASCADE ON UPDATE CASCADE,
                                start_date          TEXT,
                                start_time          TEXT,
                                end_station_id      TEXT REFERENCES stations(station_id) ON DELETE CASCADE ON UPDATE CASCADE,
                                end_date            TEXT,
                                end_time            TEXT
                                )
                            '''
    
    stationsExist = '''SELECT EXISTS(SELECT * FROM information_schema.tables WHERE table_name='stations')'''
    cur.execute(stationsExist)
    if not (cur.fetchone()[0]):
        try:
            cur.execute(createStationTableQuery)
            conn.commit()
            print('Table "stations" has been created.')
        except (Exception, 
                psycopg2.Error) as error:
            print("Error: %s" % error)
    else:
        print('Table "stations" exists.')

    journeysExist = '''SELECT EXISTS(SELECT * FROM information_schema.tables WHERE table_name='journeys')'''
    cur.execute(journeysExist)
    if not (cur.fetchone()[0]):
        try:
            cur.execute(createJourneyTableQuery)
            conn.commit()
            print('Table "journeys" has been created.')
        except (Exception, 
                psycopg2.Error) as error:
            print("Error: %s" % error)
    else:
        print('Table "journeys" exists.')
    

# Grabs data from the pandas dataframe, saves it to memory, and copies it into the "stations" table
def insertStationData(cur, conn, df):
    buffer = StringIO()
    df.to_csv(buffer, header=False)
    buffer.seek(0)

    try:
        cur.copy_from(buffer, 'stations', sep = ',')
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        return (-1)
    
    conn.commit()
    print('Inserted data in "stations".')

# Grabs data from the pandas dataframe, saves it to memory, and copies it into the "journeys" table
def insertJourneyData(cur, conn, df):
    buffer = StringIO()
    df.to_csv(buffer, header=False, index=False)
    buffer.seek(0)

    try:
        cur.copy_from(buffer, 'journeys', sep = ',', columns=['journey_id', 'journey_duration', 'start_station_id', 'start_date', 'start_time', 'end_station_id', 'end_date', 'end_time'])
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        return (-1)
    
    conn.commit()
    print('Inserted data in "journeys".')

'''
    We assume that directory location, directory name, file names, 
    and csv structures remain unaltered. 
    We assume that new data will be appended to each file.
    We assume that these are write-only csv files; meaning that data is only inserted,
    not deleted or updated. 
    We assume that the csv cannot be pre-processed and all data manipulation
    must be done using this script.
'''
def main():
    db_string = 'postgresql://postgres:password@localhost:8200/ldn_bike_db'
    engine = create_engine(db_string)
    config_db = importConfig()

    # Connect to the postgres DB
    conn = psycopg2.connect(
        user = config_db['user'],
        password = config_db['password'],
        host = config_db['host'],
        port = config_db['port'],
        dbname = config_db['dbname'],
    )
    
    # Opens a cursor to perform database operations
    cur = conn.cursor()

    createTables(cur, conn)

    print("Proceeding to process data.")
    # Create a dataframe with the stations csv
    dfStations = pd.read_csv('data/stations.csv', engine='python')

    # Create a dataframe with the journeys csv
    dfJourneys = pd.read_csv('data/journeys.csv', engine='python')

    # Remove duplicates rows from both files
    dfStations.drop_duplicates()
    dfJourneys.drop_duplicates()

    ''' Processing dataframes before insertion '''
    # Remove quotes, reformat address, and remove internal commas
    dfStations['Station Name'] = dfStations['Station Name'].str.replace('"', '')
    dfStations['Station Name'] = dfStations['Station Name'].str.split(',')
    dfStations['Station Name'] = [' '.join(reversed(station_names)) for station_names in dfStations['Station Name']]
    dfStations['Station Name'] = dfStations['Station Name'].str.replace(',', '')

    # Insert data to 'stations' table
    insertStationData(cur, conn, dfStations)

    # Rename columns to match sql schema
    dfJourneys = dfJourneys.rename(columns={'Journey ID': 'journey_id', 
                                'Journey Duration': 'journey_duration',
                                'Start Station ID': 'start_station_id',
                                'End Station ID': 'end_station_id'})

    # Formats date and time, separately, to ISO 8601 format. YY:MM:DD HH:MM in this case.
    dfJourneys['start_date'] = dfJourneys[['Start Year', 'Start Month', 'Start Date']].apply(
        lambda row: datetime.datetime.strptime(f"{row[0]:02d}", "%y").replace(month=row[1], day=row[2]).date().isoformat(),
        axis=1
    )
    dfJourneys['start_time'] = dfJourneys[['Start Year', 'Start Month', 'Start Date']].apply(
        lambda row: datetime.time(row[0], row[1], row[2]).isoformat(),
        axis=1
    )
    dfJourneys['end_date'] = dfJourneys[['End Year', 'End Month', 'End Date']].apply(
        lambda row: datetime.datetime.strptime(f"{row[0]:02d}", "%y").replace(month=row[1], day=row[2]).date().isoformat(),
        axis=1
    )
    dfJourneys['end_time'] = dfJourneys[['End Hour', 'End Minute']].apply(
        lambda row: datetime.time(row[0], row[1]).isoformat(),
        axis=1
    )

    # Remove overlapping columns from dataframe
    dfJourneys = dfJourneys.drop(['Start Year','Start Month','Start Date','Start Hour','Start Minute','Start Date','End Year','End Month','End Date','End Hour', 'End Minute'], axis=1)

    # Reorder columns to match sql schema
    dfJourneys = dfJourneys[['journey_id', 'journey_duration', 'start_station_id', 'start_date', 'start_time', 'end_station_id', 'end_date', 'end_time']]

    # Remove invalid start_station_id and end_station_id
    dfJourneys = dfJourneys[dfJourneys['start_station_id'].isin(dfStations['Station ID']) & dfJourneys['end_station_id'].isin(dfStations['Station ID'])]

    # Insert data to 'journeys' table
    insertJourneyData(cur, conn, dfJourneys)

    # Closes cursor to stop database operations
    cur.close()


if __name__ == "__main__":
    main()