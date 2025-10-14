import requests
from google.transit import gtfs_realtime_pb2
import pandas as pd
from datetime import datetime as dt

def get_english_translation(translated_text_field):

    if not translated_text_field or not translated_text_field.translation:
        return None
    
    for translation in translated_text_field.translation:
        if translation.language.upper() == 'EN':
            return translation.text
            
    return translated_text_field.translation[0].text

def fetch_and_flatten_alerts_data():
    MTA_BUS_TIME_API_KEY = "YOUR_MTA_BUS_TIME_KEY_HERE"
    ALERTS_URL = f"https://gtfsrt.prod.obanyc.com/alerts?key={MTA_BUS_TIME_API_KEY}"

    flattened_alerts = []
    feed = gtfs_realtime_pb2.FeedMessage()

    print(f"Attempting to fetch data from Alerts API...")
    try:
        response = requests.get(ALERTS_URL)
        response.raise_for_status()
        feed.ParseFromString(response.content)
        print("Successfully fetched and parsed data.")

        for entity in feed.entity:
            if not entity.HasField('alert'):
                continue

            alert = entity.alert

            alert_info = {
                "alert_id": entity.id,
                "start_time": getattr(alert.active_period[0], 'start', 0),
                "end_time": getattr(alert.active_period[0], 'end', 0),
                "header_text": get_english_translation(alert.header_text),
                "description_text": get_english_translation(alert.description_text)
            }

            for informed_entity in alert.informed_entity:
                
                entity_specific_info = {
                    "agency_id": getattr(informed_entity, 'agency_id', None),
                    "route_id": getattr(informed_entity, 'route_id', None),
                    "stop_id": getattr(informed_entity, 'stop_id', None),
                    "trip_route_id": getattr(informed_entity.trip, 'route_id', None),
                    "trip_direction_id": getattr(informed_entity.trip, 'direction_id', None)
                }

                full_row_data = {**alert_info, **entity_specific_info}
                flattened_alerts.append(full_row_data)

        print(f"Processed {len(flattened_alerts)} informed entities from {len(feed.entity)} alerts.")

    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    
    return flattened_alerts

if __name__ == "__main__":
    real_time_alerts_data = fetch_and_flatten_alerts_data()

    if len(real_time_alerts_data) > 0:
       real_time_alerts_df = pd.DataFrame(real_time_alerts_data)
       print(f"Created DataFrame with {len(real_time_alerts_df)} rows.")
       real_time_alerts_df.to_excel(r"alerts_data.xlsx", index=False)
    else:
        print("Could not create DataFrame as no data was processed.")