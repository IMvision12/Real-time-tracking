from flask import Flask, render_template, Response, send_from_directory
from pykafka import KafkaClient
from hdfs import InsecureClient
import json
import time
from datetime import datetime
import os
from threading import Lock
import logging

app = Flask(__name__, 
            static_url_path='/static',
            static_folder='static',
            template_folder='templates')

HDFS_URL = "http://localhost:9870"
HDFS_USER = "chawdagitesh"
HDFS_BUS_DIR = "/bus_tracking/B52"
UPDATE_INTERVAL = 20  # Updated to 10 seconds

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

cache = {
    'last_update': None,
    'data': None
}
cache_lock = Lock()

def get_kafka_client():
    return KafkaClient(hosts='127.0.0.1:9092')

def get_hdfs_client():
    return InsecureClient(HDFS_URL, user=HDFS_USER)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/static/<path:path>')
def send_static(path):
    return send_from_directory('static', path)

def read_bus_data():
    global cache
    
    current_time = time.time()
    
    with cache_lock:
        if (cache['last_update'] is not None and 
            current_time - cache['last_update'] < UPDATE_INTERVAL and 
            cache['data'] is not None):
            logger.debug("Returning cached data")
            return cache['data']
    
    logger.info("Reading fresh data from HDFS")
    hdfs_client = get_hdfs_client()
    combined_geojson = {
        "type": "FeatureCollection",
        "features": []
    }
    
    try:
        files = hdfs_client.list(HDFS_BUS_DIR)
        for file in files:
            if file.endswith('.json'):
                with hdfs_client.read(f"{HDFS_BUS_DIR}/{file}") as reader:
                    try:
                        bus_data = json.loads(reader.read())
                        if bus_data["features"]:
                            feature = bus_data["features"][0]
                            if feature["geometry"]["coordinates"]:
                                feature["geometry"]["coordinates"] = [feature["geometry"]["coordinates"][-1]]
                                combined_geojson["features"].append(feature)
                    except json.JSONDecodeError as e:
                        logger.error(f"Error parsing JSON from file {file}: {str(e)}")
                        continue
        
        with cache_lock:
            cache['last_update'] = current_time
            cache['data'] = combined_geojson
        
        logger.info(f"Updated data with {len(combined_geojson['features'])} buses")
        return combined_geojson
    except Exception as e:
        logger.error(f"Error reading HDFS data: {str(e)}")
        with cache_lock:
            return cache['data'] if cache['data'] is not None else combined_geojson

@app.route('/bus-data')
def get_bus_updates():
    def generate_updates():
        while True:
            try:
                bus_data = read_bus_data()
                if bus_data["features"]:
                    logger.info(f"Sending update with {len(bus_data['features'])} buses")
                    yield f"data:{json.dumps(bus_data)}\n\n"
                else:
                    logger.warning("No bus features found in data")
            except Exception as e:
                logger.error(f"Error generating updates: {str(e)}")
                yield f"data:{json.dumps({'error': str(e)})}\n\n"
            
            time.sleep(UPDATE_INTERVAL)  # Wait for 10 seconds before next update
    
    return Response(generate_updates(), mimetype="text/event-stream")

if __name__ == '__main__':
    logger.info(f"Starting server with {UPDATE_INTERVAL} second update interval")
    app.run(debug=True, port=5001)