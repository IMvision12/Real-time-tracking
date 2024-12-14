from hdfs import InsecureClient
import json
import logging
from config import HDFS_OUTPUT_DIR, HDFS_URL, HDFS_USER, PROCESSED_DATA_DIR

class HDFSManager:
    def __init__(self, logger):
        self.logger = logger
        try:
            self.client = InsecureClient(HDFS_URL, user=HDFS_USER)
            self._initialize_directories()
        except Exception as e:
            self.logger.error(f"Failed to initialize HDFS client: {str(e)}")
            raise
    
    def _initialize_directories(self):
        for directory in [HDFS_OUTPUT_DIR, PROCESSED_DATA_DIR]:
            try:
                if not self.client.status(directory, strict=False):
                    self.client.makedirs(directory)
                    self.logger.info(f"Created HDFS directory: {directory}")
            except Exception as e:
                self.logger.error(f"Failed to initialize directory {directory}: {str(e)}")
                raise
    
    def write_bus_data(self, bus_id, geojson_data):
        try:
            sanitized_bus_id = bus_id.replace(" ", "_")
            file_path = f"{HDFS_OUTPUT_DIR}/{sanitized_bus_id}.json"
            json_data = json.dumps(geojson_data, indent=2)
            
            with self.client.write(file_path, overwrite=True) as writer:
                writer.write(json_data.encode('utf-8'))
            
            self.logger.debug(f"Successfully wrote data for bus {bus_id} to HDFS")
            return True
        except Exception as e:
            self.logger.error(f"Error writing to HDFS for bus {bus_id}: {str(e)}")
            return False
        
    def write_json_data(self, filename, data):
        try:
            json_data = json.dumps(data)
            self.client.write(filename, json_data.encode('utf-8'), overwrite=True)
            self.logger.info(f"Successfully wrote JSON data to {filename}")
        except Exception as e:
            self.logger.error(f"Failed to write JSON data to {filename}: {str(e)}")
            raise
