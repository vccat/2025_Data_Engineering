import pandas as pd
from kafka import KafkaProducer
import requests
import io
import json
from time import time

t0 = time() #start time of the whole process

# 1. Download the data file
url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-10.csv.gz"
response = requests.get(url)
compressed_file = io.BytesIO(response.content)

# 2. Decompress the file and read it into a pandas DataFrame
df = pd.read_csv(
    compressed_file,
    compression='gzip', 
    parse_dates=['lpep_pickup_datetime', 'lpep_dropoff_datetime'],  
    dtype={
        'store_and_fwd_flag': 'str' 
    }
)

# 3. Keep only the required columns
required_columns = [
    'lpep_pickup_datetime',
    'lpep_dropoff_datetime',
    'PULocationID',
    'DOLocationID',
    'passenger_count',
    'trip_distance',
    'tip_amount'
]

df = df[required_columns]

# --------------------------
# 4. Clean NaN values
# --------------------------
# Replace NaN in passenger_count with 0 (or another default)
df['passenger_count'] = df['passenger_count'].fillna(0)

# Convert to integer (optional, since passenger count should be whole numbers)
df['passenger_count'] = df['passenger_count'].astype(int)

# Handle NaNs in other numeric columns (trip_distance, tip_amount)
df['trip_distance'] = df['trip_distance'].fillna(0.0)
df['tip_amount'] = df['tip_amount'].fillna(0.0)

# 5.Convert datetime columns to strings (for JSON serialization)
df['lpep_pickup_datetime'] = df['lpep_pickup_datetime'].dt.strftime('%Y-%m-%d %H:%M:%S')
df['lpep_dropoff_datetime'] = df['lpep_dropoff_datetime'].dt.strftime('%Y-%m-%d %H:%M:%S')

# 6. Initialize Kafka Producer
def json_serializer(data):
    return json.dumps(data).encode('utf-8')

server = 'localhost:9092'

producer = KafkaProducer(
    bootstrap_servers=[server],
    value_serializer=json_serializer
)

# 5. Send each row to the Redpanda topic
topic_name = "green-trips"

for index, row in df.iterrows():
    message = row.to_dict()
    producer.send(topic_name, value=message)
    if index % 1000 == 0:  # Progress tracking
        print(f"Sent {index} messages...")

producer.flush()
print("All data sent to Redpanda!")

t1 = time()
took = t1 - t0
print(f"Total time taken: {took:.2f} seconds")