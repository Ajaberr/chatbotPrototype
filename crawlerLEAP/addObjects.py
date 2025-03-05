import weaviate
from weaviate.classes.init import Auth
from weaviate.classes.config import Integrations
import json
import time

# Connect to your Weaviate Cloud instance
client = weaviate.connect_to_weaviate_cloud(
    cluster_url="https://8fzaavbvs9cmcmmhj3r9ag.c0.us-east1.gcp.weaviate.cloud",  # Replace with your Weaviate Cloud URL
    auth_credentials=Auth.api_key("UO94NG8zO65WQ7TYe6RzjbKNIwepH4TvAMa1"),           # Replace with your Weaviate Cloud key
)

# Configure integrations (Cohere in this case)
integrations = [
    Integrations.cohere(
        api_key="vaJnTfyc9YMXCBGen8l0ttVqt5O0RUrOIIaiIshg",
    ),
]
client.integrations.configure(integrations)

storage = client.collections.get("leapData")

# Load your data files
with open("Youtube_Data.json", "r", encoding="utf-8") as f:
    youtube_data_list = json.load(f)

with open("crawl_results.json", "r", encoding="utf-8") as f:
    website_data_list = json.load(f)

# Configure batch parameters
batch_size = 50     # Number of objects per batch
delay_seconds = 60   # Delay between batches (in seconds)
error_threshold = 10 # Maximum allowed errors before stopping a batch

def process_batch(data_list, data_type):
    total_objects = len(data_list)
    for i in range(0, total_objects, batch_size):
        current_batch = data_list[i:i+batch_size]
        print(f"Processing {data_type} objects {i+1} to {i+len(current_batch)} of {total_objects}...")
        with storage.batch.dynamic() as batch:
            for d in current_batch:
                # Build the object; use get() for optional keys to avoid KeyErrors
                obj = {
                    "title": d.get("title", ""),
                    "class": d.get("class", ""),
                    "videoId": d.get("videoId", ""),
                    "url": d.get("url", ""),
                    "transcript": d.get("transcript", "")
                }
                batch.add_object(obj)
                if batch.number_errors > error_threshold:
                    print("Batch import stopped due to excessive errors.")
                    break
        # Wait to avoid exceeding the rate limit
        print(f"Batch processed. Waiting {delay_seconds} seconds before next batch...")
        time.sleep(delay_seconds)

print("Processing Youtube data...")
process_batch(youtube_data_list, "Youtube")

print("Processing website data...")
process_batch(website_data_list, "Website")

client.close()  # Free up resources
