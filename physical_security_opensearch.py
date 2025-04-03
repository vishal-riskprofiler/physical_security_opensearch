
import time,json
from opensearchpy import OpenSearch ,helpers

# Replace with your OpenSearch domain
OPENSEARCH_HOST = "https://search-mydoamin-evoetoeqed4pjgil7ymxdmdlui.aos.us-east-1.on.aws"

# Authentication (IAM credentials or basic auth)
auth = ("admin", "enter your Iam password")  # If using fine-grained access control

# Create OpenSearch client
client = OpenSearch(
    hosts=[OPENSEARCH_HOST],
    http_auth=auth
)

# Index Name
index_name = "locations"

# Define Index Mapping for GeoPoint
mapping = {
    "mappings": {
        "properties": {
            "id": {"type": "integer"},
            "title": {"type": "text"},
            "description": {"type": "text"},
            "incident_type": {"type": "text"},
            "status": {"type": "text"},
            "radius": {"type": "integer"},
            "duration": {"type": "integer"},
            "location": {"type": "geo_point"}  # Ensure proper geo-point indexing
        }
    }
}

# Create Index (Ignore if it already exists)
client.indices.create(index=index_name, body=mapping, ignore=400)

# File Path for JSON Data
json_file = "bulk_locations_converted.json"

try:
    with open(json_file, "r", encoding="utf-8") as file:
        content = file.read().strip()

        if content.startswith("["):  
            data = json.loads(content)  # If JSON array
        else:  
            data = [json.loads(line) for line in content.split("\n") if line.strip()]  # If JSON lines

    # Convert geo_latitude & geo_longitude into geo_point
    def process_location_data(data):
        for record in data:
            if "geo_latitude" in record and "geo_longitude" in record:
                record["location"] = {
                    "lat": record.pop("geo_latitude"),
                    "lon": record.pop("geo_longitude")
                }
            yield {
                "_index": index_name,
                "_id": record.get("id"),
                "_source": record
            }

    # Perform Bulk Insert & Measure Execution Time
    start_time = time.time()
    success, _ = helpers.bulk(client, process_location_data(data))
    end_time = time.time()

    execution_time_ms = (end_time - start_time) * 1000  
    print(f"Inserted {success} location records in {execution_time_ms:.2f} ms")

except json.JSONDecodeError as e:
    print(f"JSON Decode Error: {e}")

except Exception as e:
    print(f"Error: {e}")

# Load JSON Data
json_file = "bulk_locations_converted.json"  # Replace with your JSON file path

try:
    with open(json_file, "r", encoding="utf-8") as file:
        content = file.read().strip()
        
        if content.startswith("["):  # JSON array format
            data = json.loads(content)
        else:  # JSON Lines format
            data = [json.loads(line) for line in content.split("\n") if line.strip()]
    
    # Define Index Name
    index_name = "incidents"

    # Bulk Insert
    def generate_bulk_data(data, index_name):
        for record in data:
            yield {
                "_index": index_name,
                "_id": record.get("id"),  
                "_source": record
            }

    start_time = time.time()
    success, _ = helpers.bulk(client, generate_bulk_data(data, index_name))
    end_time = time.time()

    execution_time_ms = (end_time - start_time) * 1000  

    print(f"Inserted {success} records in {execution_time_ms:.2f} ms")

except json.JSONDecodeError as e:
    print(f"JSON Decode Error: {e}")

# Create 'incidents' index
index_name = "incidents"
index_body = {
    "settings": {"number_of_shards": 1, "number_of_replicas": 0},
    "mappings": {
        "properties": {
            "id": {"type": "keyword"},
            "title": {"type": "text"},
            "description": {"type": "text"},
            "location": {"type": "text"},
            "incident_type": {"type": "text"},
            "status": {"type": "text"},
            "radius": {"type": "integer"},
            "duration": {"type": "integer"},
            "geo_location": {"type": "geo_point"}
        }
    }
}

# Create index
response = client.indices.create(index=index_name, body=index_body, ignore=400)
print(f"Index creation response: {response}")

# Add sample data
sample_data = {
    "id": "1",
    "title": "Gas Leak",
    "description": "Gas leak detected in Sector 15.",
    "location": "Sector 15",
    "incident_type": "Hazard",
    "status": "Open",
    "radius": 5,
    "duration": 120,
    "geo_location": {"lat": 28.7041, "lon": 77.1025}
}

response = client.index(index=index_name, body=sample_data)
print(f"Data Insertion Response: {response}")



# Function to measure execution time
def execute_query(index, query):
    start_time = time.time()  # Start time in seconds

    response = client.search(index=index, body=query)

    end_time = time.time()  # End time in seconds
    execution_time_ms = (end_time - start_time) * 1000  # Convert to milliseconds

    print(f"Query Execution Time: {execution_time_ms:.2f} ms")
    return response

# Fetch All Incidents
query_all = {"query": {"match_all": {}}}
response_all = execute_query("incidents", query_all)
print("All Incidents:", response_all["hits"]["hits"])

# Search by Incident Type
query_type = {"query": {"match": {"incident_type": "Explosion Threat"}}}
response_type = execute_query("incidents", query_type)
print("Filtered Incidents:", response_type["hits"]["hits"])

# Geo Query: Fetch Nearby Incidents
query_geo = {
    "query": {
        "bool": {
            "filter": {
                "geo_distance": {
                    "distance": "5km",
                    "geo_location": {"lat": 28.7041, "lon": 77.1025}
                }
            }
        }
    }
}
response_geo = execute_query("incidents", query_geo)
print("Nearby Incidents:", response_geo["hits"]["hits"])