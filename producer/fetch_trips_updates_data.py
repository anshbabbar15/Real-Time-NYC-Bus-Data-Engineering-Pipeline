import requests
from google.transit import gtfs_realtime_pb2
import pandas as pd
from datetime import datetime as dt

def fetch_and_flatten_trip_updates_data():
    MTA_BUS_TIME_API_KEY = "YOUR_MTA_BUS_TIME_KEY_HERE" 
    TRIP_UPDATES_URL = f"https://gtfsrt.prod.obanyc.com/tripUpdates?key={MTA_BUS_TIME_API_KEY}"

    flattened_data = []
    feed = gtfs_realtime_pb2.FeedMessage()
    print(f"Attempting to fetch data from Trip updates API...")
    try:
        response = requests.get(TRIP_UPDATES_URL)
        response.raise_for_status()
        feed.ParseFromString(response.content)
        print("Successfully fetched and parsed data.")

        for entity in feed.entity:
            if not entity.HasField('trip_update'):
                continue
            
            trip_update = entity.trip_update

            trip_info = {
                "trip_id": getattr(trip_update.trip, 'trip_id', None),
                "start_date": getattr(trip_update.trip, 'start_date', None),
                "route_id": getattr(trip_update.trip, 'route_id', None),
                "direction_id": getattr(trip_update.trip, 'direction_id', None),
                "vehicle_id": getattr(trip_update.vehicle, 'id', None),
                "update_timestamp": getattr(trip_update, 'timestamp', 0),
                "trip_delay_seconds":  getattr(trip_update, 'delay', None)                
            }

            for stop_update in trip_update.stop_time_update:
                stop_specific_info = {
                    "stop_sequence": getattr(stop_update, 'stop_sequence', None),
                    "stop_id": getattr(stop_update, 'stop_id', None),
                    "arrival_time": getattr(stop_update.arrival, 'time', 0),
                    "departure_time": getattr(stop_update.departure, 'time', 0),
                }

                full_row_data = {**trip_info, **stop_specific_info}
                flattened_data.append(full_row_data)

        print(f"Processed {len(flattened_data)} stop predictions from {len(feed.entity)} trip updates.")

    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    
    return flattened_data

if __name__ == "__main__":
    trip_updates_data = fetch_and_flatten_trip_updates_data()

    if len(trip_updates_data) > 0:
       trip_updates_df = pd.DataFrame(trip_updates_data)
       print(f"Created DataFrame with {len(trip_updates_df)} rows.")
       trip_updates_df.to_excel(r"trip_updates_data.xlsx", index=False)
    else:
        print("Could not create DataFrame as no data was processed.")