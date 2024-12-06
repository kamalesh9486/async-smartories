import couchdb
import datetime
import time
import asyncio
import json
import gmqtt
import math
import pandas as pd


file_path = "/home/pi/Music/myenv/Asite1_m1_modified.csv"
df = pd.read_csv(file_path)

# Drop the 'data.downtime_status' column if it exists
if 'data.downtime_status' in df.columns:
    df.drop(columns=['data.downtime_status'], inplace=True)

# MQTT setup
broker = "broker.emqx.io"
port = 1883
data_topic = "try"
ack_topic = "try_ack"
heartbeat_topic = "try_heartbeat"

# In-memory tracking and constants
heartbeat_interval = 10
unacknowledged_ids = set()
received_acknowledgments_set = set()
receiver_online = False

# CouchDB setup
couch_server_url = "http://127.0.0.1:5984/"
username = "admin"
password = "admin"
couch = couchdb.Server(couch_server_url)
couch.resource.credentials = (username, password)
couch = couchdb.Server('http://admin:admin@127.0.0.1:5984')

# Check if the database exists, if not create it
if 'main_db1' not in couch:
    couch.create('main_db1')
    
# Now access the main_db
main_db = couch['main_db1']
# Ensure databases exist
def ensure_couchdb_databases():
    db_names = ["mqtt_trying", "acknowledgments", "acknowledgment_db"]
    for db_name in db_names:
        if db_name not in couch:
            couch.create(db_name)

ensure_couchdb_databases()

def sanitize_data(data):
    """Ensure all values in the data are JSON-compliant."""
    sanitized = {}
    for key, value in data.items():
        if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
            sanitized[key] = None  # Replace invalid floats with None
        else:
            sanitized[key] = value
    return sanitized

def store_in_acknowledgment_db(data):
    """Store data in the acknowledgment database with sanitization."""
    ack_db = couch['acknowledgment_db']
    sanitized_data = sanitize_data(data)
    doc_id = sanitized_data['_id']
    if doc_id in ack_db:
        existing_doc = ack_db[doc_id]
        for key, value in sanitized_data.items():
            existing_doc[key] = value
        ack_db.save(existing_doc)
    else:
        ack_db[doc_id] = sanitized_data

def insert_into_db(row):
    """Insert sanitized data into the main database."""
    main_db = couch['main_db1']
    sanitized_row = sanitize_data(row)  # Sanitize the row before insertion
    doc_id = sanitized_row['_id']
    if doc_id in main_db:
        existing_doc = main_db[doc_id]
        for key, value in sanitized_row.items():
            existing_doc[key] = value
        main_db.save(existing_doc)
    else:
        main_db[doc_id] = sanitized_row

# MQTT client setup with reconnect handling
mqtt_client = gmqtt.Client("mqtt_client_{}".format(datetime.datetime.now().timestamp()))

# MQTT connection handling
async def reconnect_mqtt():
    max_retries = 5
    delay = 2  # Start with a 2-second delay
    for attempt in range(1, max_retries + 1):
        try:
            print(f"Reconnecting attempt {attempt}...")
            await mqtt_client.connect(broker, port)
            print("Reconnected successfully!")
            return
        except Exception as e:
            print(f"Reconnect failed (attempt {attempt}): {e}")
            await asyncio.sleep(delay)
            delay *= 2  # Exponential backoff
    print("Max reconnection attempts reached. Exiting...")

mqtt_client.on_connect = lambda client, flags, result, properties: asyncio.create_task(handle_connect(client, flags, result, properties))
mqtt_client.on_disconnect = lambda client, packet, exc=None: asyncio.create_task(reconnect_mqtt())

async def handle_connect(client, flags, result, properties):
    print("Connected to MQTT Broker!")
    client.subscribe(ack_topic)

async def on_ack_message(client, topic, packet, qos, properties):
    global receiver_online
    ack_data = json.loads(packet.decode())
    ack_id = ack_data.get('_id')

    if ack_id == "heartbeat" and ack_data.get('ack') == 'received':
        receiver_online = True
        print("Heartbeat acknowledged, receiver is online.")
    elif ack_data.get('ack') == 'received' and ack_id:
        received_acknowledgments_set.add(ack_id)
        print(f"Acknowledgment received for data _id: {ack_id}")

        # Remove acknowledged data from acknowledgment_db
        ack_db = couch['acknowledgment_db']
        if ack_id in ack_db:
            del ack_db[ack_id]
            print(f"Removed acknowledged data _id {ack_id} from acknowledgment_db.")


mqtt_client.on_message = on_ack_message

async def safe_publish(topic, message, retries=3):
    for attempt in range(retries):
        try:
            mqtt_client.publish(topic, message, qos=1)
            print(f"Published to {topic}: {message}")
            return
        except Exception as e:
            print(f"Publish failed (attempt {attempt+1}): {e}")
            await asyncio.sleep(2)

async def send_current_data():
    while not df.empty:
        current_time = datetime.datetime.now().time()
        
        for index, row in df.iterrows():
            target_time = datetime.datetime.strptime(row['Time'], '%I:%M:%S %p').time()
            if (current_time.hour == target_time.hour and
                current_time.minute == target_time.minute and
                current_time.second == target_time.second):

                combined_datetime = f"{row['Date']} {row['Time']}"
                row['updated_on'] = combined_datetime
                row_data = row.drop(['Date', 'Time']).to_dict()
                message = json.dumps(row_data)
                
                await safe_publish(data_topic, message)
                
                start_time = time.time()
                acknowledgment_received = False
                while time.time() - start_time < 2:
                    if row_data['_id'] in received_acknowledgments_set:
                        acknowledgment_received = True
                        print(f"Acknowledgment received for data _id {row_data['_id']}")
                        break
                    await asyncio.sleep(0.1)

                if not acknowledgment_received:
                    print(f"No acknowledgment for data _id {row_data['_id']}, storing in acknowledgment_db")
                    store_in_acknowledgment_db(row_data)

                insert_into_db(row_data)
                df.drop(index, inplace=True)
                break

        await asyncio.sleep(0.5)

async def send_heartbeat():
    while True:
        heartbeat_message = json.dumps({"_id": "heartbeat", "ack": "ping"})
        await safe_publish(heartbeat_topic, heartbeat_message)
        await asyncio.sleep(heartbeat_interval)

async def resend_unsent_data():
    global receiver_online
    while True:
        if receiver_online:
            receiver_online = False
            ack_db = couch['acknowledgment_db']
            for doc_id in list(ack_db):
                data = ack_db.get(doc_id, None)  # Safely get the document
                if data and data['_id'] not in received_acknowledgments_set:
                    message = json.dumps(data)
                    await safe_publish(data_topic, message)

                    start_time = time.time()
                    acknowledgment_received = False
                    while time.time() - start_time < 2:
                        if data['_id'] in received_acknowledgments_set:
                            acknowledgment_received = True
                            print(f"Acknowledgment received for data _id {data['_id']}, deleting from acknowledgment_db")
                            if doc_id in ack_db:  # Check if the document still exists
                                del ack_db[doc_id]
                            break
                        await asyncio.sleep(0.1)
        await asyncio.sleep(heartbeat_interval)


async def main():
    await mqtt_client.connect(broker, port)
    await asyncio.gather(
        send_current_data(),
        send_heartbeat(),
        resend_unsent_data()
    )

if __name__ == "__main__":
    asyncio.run(main())
