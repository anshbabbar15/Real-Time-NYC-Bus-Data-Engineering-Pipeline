import json
import time
from azure.eventhub import EventHubProducerClient, EventData


from fetch_bus_positions_data import fetch_and_flatten_bus_positions_data
from fetch_trips_updates_data import fetch_and_flatten_trip_updates_data
from fetch_alerts_data import fetch_and_flatten_alerts_data
# --- Configuration ---
AZURE_CONNECTION_STR = "My-connection-string"
AZURE_EVENTHUB_NAME = "bus-positions-topic" 

# The official limit is 1048576 bytes. We use a slightly smaller number for a safety buffer.
MAX_PAYLOAD_SIZE_BYTES = 1040000 

if __name__ == "__main__":
    # Create the producer client once at the start
    producer_client = EventHubProducerClient.from_connection_string(
        conn_str=AZURE_CONNECTION_STR,
        eventhub_name=AZURE_EVENTHUB_NAME
    )
    
    print("Starting the unified real-time producer. Press Ctrl+C to stop.")
    
    try:
        while True:
            all_messages_for_batch = []

            # --- Fetch Trip Updates ---
            trip_updates = fetch_and_flatten_trip_updates_data()
            if trip_updates:
                for update in trip_updates:
                    # Add the crucial 'message_type' field
                    update['message_type'] = 'trip_update' 
                    all_messages_for_batch.append(update)

            # --- Fetch Vehicle Positions ---
            bus_positions = fetch_and_flatten_bus_positions_data()
            if bus_positions:
                for position in bus_positions:
                    # Add the 'message_type' field
                    position['message_type'] = 'bus_position'
                    all_messages_for_batch.append(position)

            # --- Fetch Alerts ---
            alerts = fetch_and_flatten_alerts_data()
            if alerts:
                for alert in alerts:
                    # Add the 'message_type' field
                    alert['message_type'] = 'alert'
                    all_messages_for_batch.append(alert)
            
            if all_messages_for_batch:
                print(f"Preparing to send a total of {len(all_messages_for_batch)} messages...")
                with producer_client:
                    # Start with a new, empty batch
                    event_data_batch = producer_client.create_batch()
                    
                    for message_dict in all_messages_for_batch:
                        json_payload = json.dumps(message_dict)

                        # Check the size of the encoded payload string.
                        payload_size_in_bytes = len(json_payload.encode('utf-8'))

                        if payload_size_in_bytes > MAX_PAYLOAD_SIZE_BYTES:
                            print(f"WARNING: Skipping a message of type '{message_dict.get('message_type')}' because its payload is too large ({payload_size_in_bytes} bytes).")
                            continue # Skip to the next message

                        event_data = EventData(json_payload)
                        
                        # Try to add the event to the current batch
                        try:
                            event_data_batch.add(event_data)
                        except ValueError:
                            # The current batch is full. Send it.
                            print(f"Batch is full. Sending batch with {len(event_data_batch)} messages.")
                            producer_client.send_batch(event_data_batch)

                            time.sleep(4) # PAUSE FOR 4 SECOND AFTER SENDING A BATCH TO AVOID THROTTLING
                            
                            # Create a new, empty batch for the next set of messages
                            event_data_batch = producer_client.create_batch()
                            
                            # Now, add the event that was too large for the previous batch
                            event_data_batch.add(event_data)

                    # After the loop, there might be a final batch of messages that hasn't been sent yet.
                    if len(event_data_batch) > 0:
                        print(f"Sending the final batch with {len(event_data_batch)} messages.")
                        producer_client.send_batch(event_data_batch)
                        
                    print("All messages have been sent successfully.")
            else:
                print("No data fetched in this cycle.")

            # --- Wait before the next cycle ---
            print("Waiting for 60 seconds...")
            time.sleep(60)
            
    except KeyboardInterrupt:
        print("Producer stopped by user.")
    finally:
        print("Shutting down producer.")