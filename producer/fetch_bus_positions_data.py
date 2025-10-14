import requests
from google.transit import gtfs_realtime_pb2
import pandas as pd
from datetime import datetime as dt


def fetch_and_flatten_bus_positions_data():
    MTA_BUS_TIME_API_KEY = "cf458fc9-a12f-4c4f-9b8c-9d871c87056f" 
    BUS_DATA_URL =  f"https://gtfsrt.prod.obanyc.com/vehiclePositions?key={MTA_BUS_TIME_API_KEY}"

    flattened_data =[]
    feed = gtfs_realtime_pb2.FeedMessage()
    print(f"Attempting to fetch data from Bus positions API...")
    try:
        response = requests.get(BUS_DATA_URL)
        response.raise_for_status() 
        feed.ParseFromString(response.content)
        print("Successfully fetched and parsed bus position data.")

        for entity in feed.entity:
            if not entity.HasField('vehicle'):
                continue

            vehicle = entity.vehicle
            vehicle_info_row = {
                "vehicle_id": getattr(getattr(vehicle, "vehicle", None), "id", None),
                "trip_id": getattr(getattr(vehicle, "trip", None), "trip_id", None),
                "start_date": getattr(getattr(vehicle, "trip", None), "start_date", None),
                "route_id": getattr(getattr(vehicle, "trip", None), "route_id", None),
                "direction_id": getattr(getattr(vehicle, "trip", None), "direction_id", None),
                "stop_id": getattr(vehicle, "stop_id", None),
                "latitude": getattr(getattr(vehicle, "position", None), "latitude", None),
                "longitude": getattr(getattr(vehicle, "position", None), "longitude", None),
                "bearing": getattr(getattr(vehicle, "position", None), "bearing", None),
                "timestamp": getattr(vehicle, "timestamp", None),
            }
            flattened_data.append(vehicle_info_row)
        
        print(f"Processed {len(flattened_data)} bus positions data from {len(feed.entity)} available real time data.")
          
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

    return flattened_data

if __name__ == "__main__":
    bus_position_data = fetch_and_flatten_bus_positions_data()

    if len(bus_position_data) > 0:
       bus_position_df = pd.DataFrame(bus_position_data)
       print(f"Created DataFrame with {len(bus_position_df)} rows.")
       bus_position_df.to_excel(r"bus_positions_data.xlsx", index=False)
    else:
        print("Could not create DataFrame as no data was processed.")
