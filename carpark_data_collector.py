import os
import datetime
import time
import json
import sqlite3
import pandas as pd
import httplib2

# Load Datamall API key from config.json
# Note: Create config.json with own API key based on config.template.json
with open(os.path.join(os.path.dirname(__file__), 'config.json')) as config_file:
    config = json.load(config_file)
    api_key = config['datamall_api']['api_key']

# Datamall API settings
headers = {
    'AccountKey': api_key,
    'accept': 'application/json'}
url_base = 'http://datamall2.mytransport.sg/ltaodataservice/CarParkAvailabilityv2?$skip='
method = 'GET'
body = ''

# Create in-memory database
conn = sqlite3.connect(':memory:')

# Start iterations
n_count = 0
try:
    while True:
        # Get time
        time_stamp = datetime.datetime.now()
        is_end_of_week = (
            time_stamp.isoweekday() == 7
            and time_stamp.hour == 23
            and time_stamp.minute in range(50, 60)
        )
        print('Loading: ' + time_stamp.strftime('%Y-%m-%d %H:%M:%S'))

        # Iterate through API data items (max. 500 items at a time)
        finding_data = True
        n_skip = 0
        while finding_data:
            try:
                # Get JSON data from API
                response, content = httplib2.Http().request(
                    url_base + str(n_skip),
                    method,
                    body,
                    headers
                )
                data = json.loads(content)

                # Check if any data was found
                data = pd.read_json(json.dumps(data))
                if len(data['value']) == 0:
                    finding_data = False
                else:
                    # Convert and cleanup data
                    data = data['value'].to_json(orient='values')
                    data = pd.read_json(data, orient='records')
                    data['Timestamp'] = time_stamp

                    # Add data to in-memory database
                    data.to_sql(
                        'carpark_availability_timeseries',
                        con=conn,
                        if_exists='append'
                    )
                    n_skip += 500
            except (KeyboardInterrupt, SystemExit):
                raise
            except:
                # If any API errors, wait some seconds and retry
                print('Failed: ' + time_stamp.strftime('%Y-%m-%d %H:%M:%S'))
                print('Waiting to retry...')
                time.sleep(30)
                continue

        # Update CSV files with latest data (every 0 iterations)
        if ((n_count == 0) or is_end_of_week):
            try:
                print('Updating CSV...')
                # Get data from in-memory database and clean up
                data = pd.read_sql(
                    'SELECT * FROM carpark_availability_timeseries',
                    con=conn
                )
                data = data[:][data['LotType'] == 'C']
                data = data.dropna(subset=['Timestamp', 'CarParkID', 'AvailableLots'])

                # If any new carparks appeared, add to existing carpark data
                carparks_new = data[['CarParkID', 'Location', 'Agency', 'Development']]
                carparks_new = carparks_new.drop_duplicates('CarParkID')
                try:
                    carparks = pd.read_csv(
                        os.path.join(os.path.dirname(__file__), 'data', 'carparks.csv')
                    )
                except FileNotFoundError:
                    carparks = pd.DataFrame(columns=['CarParkID', 'Location', 'Agency', 'Development'])
                    carparks.to_csv(
                        os.path.join(os.path.dirname(__file__), 'data', 'carparks.csv'),
                        mode='w',
                        header=True,
                        index=False
                    )
                carparks_new = carparks.append(carparks_new, sort=True)
                carparks_new['CarParkID'] = carparks_new['CarParkID'].astype(str)
                carparks_new = carparks_new.sort_values('CarParkID')
                carparks_new = carparks_new.drop_duplicates('CarParkID')
                carparks_new = carparks_new[['CarParkID', 'Location', 'Agency', 'Development']]

                # Append carpark CSV
                carparks_new.to_csv(
                    os.path.join(os.path.dirname(__file__), 'data', 'carparks.csv'),
                    mode='w',
                    header=True,
                    index=False
                )

                # Reformat data for available_lots (Remove some columns, pivot table)
                available_lots_new = data[['Timestamp', 'CarParkID', 'AvailableLots']]
                available_lots_new = pd.pivot_table(available_lots_new, index='Timestamp', columns='CarParkID', values='AvailableLots')
                # Add exisiting data from available_lots CSV
                try:
                    available_lots = pd.read_csv(
                        os.path.join(os.path.dirname(__file__), 'data', 'available_lots.csv'),
                        index_col='Timestamp'
                    )
                    available_lots_new = available_lots.append(available_lots_new, sort=True)
                except FileNotFoundError:
                    pass
                # Resample timestamps to 10 min interval
                available_lots_new.index = pd.to_datetime(available_lots_new.index)
                available_lots_new = available_lots_new.resample('10T').mean().round()

                # Rewrite available_lots CSV
                if not is_end_of_week:
                    available_lots_new.to_csv(
                        os.path.join(os.path.dirname(__file__), 'data', 'available_lots.csv'),
                        mode='w',
                        header=True,
                        index=True
                    )
                else:
                    # Every Sunday after 23:50:00, create a available_lots CSV for the week
                    available_lots_new.to_csv(
                        os.path.join(
                            os.path.dirname(__file__),
                            'data', (
                                'available_lots_'
                                + available_lots_new.index[0].strftime('%Y-%m-%d')
                                + '_to_'
                                + available_lots_new.index[-1].strftime('%Y-%m-%d')
                                + '.csv'
                            )
                        ),
                        mode='w',
                        header=True,
                        index=True
                    )
                    try:
                        # Clear previous available_lots CSV
                        os.remove(os.path.join(os.path.dirname(__file__), 'data', 'available_lots.csv'))
                    except FileNotFoundError:
                        pass
                    if time_stamp.minute == 50:
                        # If its between 23:50 and 23:51, wait one more minute to avoid overwriting
                        time.sleep(1*60)
            except KeyError:
                print('Updating CSV failed.')
                pass

            # Reset counter and database connection
            n_count = -1
            conn.close()
            conn = sqlite3.connect(':memory:')

        # Iterate counter and wait (9 minutes)
        print('Waiting...')
        n_count = n_count+1
        time.sleep(9*60)

except (KeyboardInterrupt, SystemExit):
    # Close database connection before exiting
    conn.close()
    raise
