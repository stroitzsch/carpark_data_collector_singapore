# Singapore carpark availability data

Carpark availability data collected from Datamall API at <https://www.mytransport.sg/content/mytransport/home/dataMall.html>.

- `available_lots_XXX_to_XXX.csv`: Raw carpark availability data as obtained from Datamall for given time period in 10 min resolution. Erroneous values are kept as returned from API, therefore requiring clean-up before use.
- `carparks.csv`: List of all unique carparks with carpark ID / location / development name.
