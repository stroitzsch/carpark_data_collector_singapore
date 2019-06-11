import os
import glob
import pandas as pd
import numpy as np

# Load carpark information, resolve location and remove faulty entries.
carparks = pd.read_csv(os.path.join(os.path.dirname(__file__), 'data', 'carparks.csv'))
carparks = carparks[['CarParkID','Location']]
carparks = carparks.sort_values('CarParkID')
carparks = carparks.drop_duplicates('CarParkID')
carparks = carparks.dropna()
carparks['carpark_id'] = carparks['CarParkID']
carparks[['latitude', 'longitude']] = carparks['Location'].str.split(' ', expand=True)
carparks = carparks[['carpark_id', 'latitude', 'longitude']]

# Determine maximum capacity based on available_lots.
available_lots_files = glob.glob(os.path.join(os.path.dirname(__file__), 'data', 'available_lots_*.csv'))
# Note: This expects all carpark_id (column names) to be present in the most recent available_lots.
available_lots = pd.read_csv(available_lots_files[-1])
carparks_max = available_lots.max(axis=0)[1:]
for file_path in available_lots_files[:-1]:
    available_lots = pd.read_csv(file_path)
    carparks_max_next = available_lots.max(axis=0)[1:]
    carparks_max_next = carparks_max_next.reindex(carparks_max.index, fill_value=0)
    carparks_max = pd.concat([carparks_max, carparks_max_next], axis=1, sort=False).max(axis=1)
carparks_max = carparks_max + 10  # Assuming a minimum occupancy of any carpark.
carparks_max = carparks_max[carparks['carpark_id']]
carparks['capacity'] = carparks_max.values
carparks['num_charging_lots'] = carparks_max.values  # For future usage.

# Write CSV file.
carparks.to_csv(os.path.join(os.path.dirname(__file__), 'data', 'carparks_clean.csv'), index=False)
