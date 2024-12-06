import paho.mqtt.client as mqtt
import couchdb
import json
from datetime import datetime

# MQTT setup
broker = "broker.emqx.io"
port = 1883
data_topic = "try"
ack_topic = "try_ack"
heartbeat_topic = "try_heartbeat"

# CouchDB setup
def create_couchdb_connection():
    couch = couchdb.Server('http://kamalesh:kamalesh@127.0.0.1:5984')  # Update with your CouchDB credentials
    return couch

# Access or create the 'main_db' database
def get_main_db():
    couch = create_couchdb_connection()
    if 'main_db' not in couch:
        couch.create('main_db')  # Create the database if it doesn't exist
    return couch['main_db']

# Store received data in CouchDB
def store_received_data(data):
    main_db = get_main_db()

    # Prepare data to be stored
    doc_id = data.get('_id')
    
    if doc_id:
        data_section = data.get('data', {})  # Safely access 'data' or use an empty dictionary if it doesn't exist
        doc = {
            '_id': doc_id,
            'data_machine_id': data_section.get('machine_id'),  # Use .get() to avoid KeyError
            'data_machine_status': data_section.get('machine_status'),
            'data_shot_count': data_section.get('shot_count'),
            'data_shot_status': data_section.get('shot_status'),
            'data_status': data_section.get('status'),
            'updated_on': data.get('updated_on')  # Assuming updated_on is a datetime string
        }

        try:
            main_db[doc_id] = doc  # Insert or update the document in the database
            print(f"Data for _id {doc_id} stored in CouchDB.")
        except couchdb.http.ResourceConflict:
            print(f"Document with _id {doc_id} already exists in the database. Updating...")
            main_db[doc_id] = doc  # Update the existing document if necessary

# MQTT client setup
client = mqtt.Client()

# Callback for when the client receives a connection response
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connected to MQTT Broker!")
        client.subscribe([(data_topic, 1), (heartbeat_topic, 1)])
    else:
        print(f"Failed to connect, return code {rc}")

# Handle incoming data and heartbeat messages
def on_message(client, userdata, message):
    try:
        payload = json.loads(message.payload.decode())
        print(payload)
        
        # Handle heartbeat messages
        if message.topic == heartbeat_topic:
            heartbeat_id = payload.get('_id')
            if heartbeat_id:
                # Send acknowledgment for heartbeat
                ack_message = json.dumps({"_id": heartbeat_id, "ack": "received"})
                client.publish(ack_topic, ack_message, qos=1)
                print("Heartbeat acknowledged.")
        
        # Handle data messages
        elif message.topic == data_topic:
            # Store received data in CouchDB
            data_id = payload.get('_id')
            if data_id:
                store_received_data(payload)
                
                # Send acknowledgment for the data
                ack_message = json.dumps({"_id": data_id, "ack": "received"})
                client.publish(ack_topic, ack_message, qos=1)
                print(f"Data received and acknowledged for _id: {data_id}")
    
    except json.JSONDecodeError as e:
        print(f"Failed to decode message: {e}")

client.on_connect = on_connect
client.on_message = on_message
client.connect(broker, port)
client.loop_forever()
