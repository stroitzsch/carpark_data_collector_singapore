import os
import pandas as pd

carparks = pd.read_csv(os.path.join(os.path.dirname(__file__), 'data', 'carparks.csv'))
carparks = carparks[['CarParkID','Location']]
carparks = carparks.sort_values('CarParkID')
carparks = carparks.drop_duplicates('CarParkID')
carparks = carparks.dropna()
carparks['id'] = carparks['CarParkID']
carparks[['latitude', 'longitude']] = carparks['Location'].str.split(' ', expand=True)
carparks = carparks[['id', 'latitude', 'longitude']]
carparks.to_csv(os.path.join(os.path.dirname(__file__), 'data', 'carparks_clean.csv'), index=False)
