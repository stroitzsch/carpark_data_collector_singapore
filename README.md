# Carpark Data Collector Singapore

Python tool for collecting / downloading carpark availability data for Singapore from the [Datamall API](https://www.mytransport.sg/datamall) of Singapore's Land Transport Authority (LTA). A Datamall API key (currently free) is needed for downloads from the API with this tool.

Collected sample data is included in [`data`](data).

## Installation

1. Requirement: Python 3.6
1. Clone or download this repository.
2. In your Python environment, run `pip install -r path_to_repository/requirements.txt`.

## Usage

1. Copy and rename `config.template.json` to `config.json`.
2. Obtain API key from [Datamall API](https://www.mytransport.sg/datamall) and add the key at `"api_key": ""` in `config.json`.
3. In your Python environment, run `python carpark_data_downloader.py`.
4. Collected data is stored in [`data`](data).