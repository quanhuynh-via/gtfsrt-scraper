# GTFS-RT Scraper

The script scraper.py polls RT data from a couple transit authorities and store them in the appropriate directories (vehicle, alert, trip_update). This script assumes the directories already exist.

## Prerequisites
- python3

- `gtfs-realtime-bindings` package
```
pip install --upgrade gtfs-realtime-bindings
```
- AC Transit token (if polling AC Transit, register for token [here](http://api.actransit.org/transit/Account/Register); if not, comment out in `endpoint_mapping` in `scraper.py`

## Running
The script runs as a one-time read
```
python scraper.py
```
