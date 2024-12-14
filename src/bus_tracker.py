from datetime import datetime
import asyncio
import aiohttp
import traceback
import json
import signal
from collections import defaultdict
from config import *
from data_fetcher import *
from hdfs_client import *

# Spark imports
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, max, min, count

from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType

schema = StructType([
    StructField("timestamp", StringType(), True),
    StructField("location", StructType([
        StructField("latitude", FloatType(), True),
        StructField("longitude", FloatType(), True)
    ]), True),
    StructField("journey_details", StructType([
        StructField("LineRef", StringType(), True),
        StructField("DirectionRef", StringType(), True),
        StructField("PublishedLineName", StringType(), True),
        StructField("OriginRef", StringType(), True),
        StructField("DestinationRef", StringType(), True),
        StructField("DestinationName", StringType(), True),
        StructField("OperatorRef", StringType(), True),
        StructField("Progress", StringType(), True),
        StructField("Bearing", StringType(), True),
        StructField("Occupancy", StringType(), True),
        StructField("Delay", StringType(), True)
    ]), True),
    StructField("stop_details", StringType(), True)
])

class BusTracker:
    def __init__(self):
        self.logger = setup_logging()
        self.bus_data = {}
        self.metadata = defaultdict(lambda: {
            'start_time': datetime.now().isoformat(),
            'buses': defaultdict(list)
        })
        self.running = True
        try:
            self.hdfs_manager = HDFSManager(self.logger)
            # Set up signal handlers for graceful shutdown
            signal.signal(signal.SIGINT, self.signal_handler)
            signal.signal(signal.SIGTERM, self.signal_handler)
        except Exception as e:
            self.logger.error(f"Failed to initialize BusTracker: {str(e)}")
            raise

    def signal_handler(self, signum, frame):
        self.logger.info(f"Received signal {signum}. Starting graceful shutdown...")
        self.running = False

    def get_journey_details(self, journey):
        return {
            'LineRef': journey.get('LineRef', 'N/A'),
            'DirectionRef': journey.get('DirectionRef', 'N/A'),
            'PublishedLineName': journey.get('PublishedLineName', 'N/A'),
            'OriginRef': journey.get('OriginRef', 'N/A'),
            'DestinationRef': journey.get('DestinationRef', 'N/A'),
            'DestinationName': journey.get('DestinationName', 'N/A'),
            'OperatorRef': journey.get('OperatorRef', 'N/A'),
            'Progress': journey.get('Progress', 'N/A'),
            'Bearing': journey.get('Bearing', 'N/A'),
            'Occupancy': journey.get('Occupancy', 'N/A'),
            'Delay': journey.get('Delay', 'N/A')
        }

    def get_stop_details(self, journey):
        """Extract detailed stop information"""
        try:
            if 'OnwardCalls' in journey and journey['OnwardCalls']:
                stops = journey['OnwardCalls']['OnwardCall']
                return [{
                    'StopPointName': stop.get('StopPointName', 'N/A'),
                    'StopPointRef': stop.get('StopPointRef', 'N/A'),
                    'VisitNumber': stop.get('VisitNumber', 'N/A'),
                    'ExpectedArrivalTime': stop.get('ExpectedArrivalTime', 'N/A'),
                    'ExpectedDepartureTime': stop.get('ExpectedDepartureTime', 'N/A'),
                    'Distance': stop.get('Extensions', {}).get('Distances', {}).get('PresentableDistance', 'N/A'),
                    'StopStatus': stop.get('Extensions', {}).get('StopStatus', 'N/A')
                } for stop in stops]
            return []
        except KeyError:
            return []

    def process_vehicle_data(self, vehicle):
        try:
            journey = vehicle['MonitoredVehicleJourney']
            location = journey['VehicleLocation']
            timestamp = vehicle['RecordedAtTime']
            bus_id = journey.get('VehicleRef', 'unknown')
            
            new_coord = [
                float(location['Longitude']),
                float(location['Latitude'])
            ]
            
            if bus_id not in self.bus_data:
                self.bus_data[bus_id] = {
                    "type": "FeatureCollection",
                    "features": [{
                        "type": "Feature",
                        "properties": {
                            "bus_id": bus_id,
                            "timestamps": [timestamp]
                        },
                        "geometry": {
                            "type": "LineString",
                            "coordinates": [new_coord]
                        }
                    }]
                }
            else:
                feature = self.bus_data[bus_id]["features"][0]
                feature["properties"]["timestamps"].append(timestamp)
                feature["geometry"]["coordinates"].append(new_coord)
            
            metadata = {
                'timestamp': timestamp,
                'location': {
                    'latitude': location['Latitude'],
                    'longitude': location['Longitude']
                },
                'journey_details': self.get_journey_details(journey),
                'stop_details': self.get_stop_details(journey)
            }
            
            self.metadata['session_data']['buses'][bus_id].append(metadata)
            
            return bus_id, self.bus_data[bus_id]
            
        except (KeyError, ValueError, TypeError) as e:
            self.logger.error(f"Error processing vehicle data: {str(e)}")
            self.logger.debug(f"Problematic vehicle data: {vehicle}")
            return None, None

    def save_session_metadata(self):
        self.metadata['session_data']['end_time'] = datetime.now().isoformat()
        
        start_time = datetime.fromisoformat(self.metadata['session_data']['start_time'])
        end_time = datetime.fromisoformat(self.metadata['session_data']['end_time'])
        duration = (end_time - start_time).total_seconds()
        self.metadata['session_data']['duration_seconds'] = duration

        # Spark-powered metadata aggregation
        spark = SparkSession.builder \
            .appName("BusMetadataAggregation") \
            .getOrCreate()

        try:
            # Aggregate bus metadata using Spark
            for bus_id, updates in self.metadata['session_data']['buses'].items():
                # Convert updates to a DataFrame for Spark processing
                bus_df = spark.createDataFrame(updates, schema)

                # Perform aggregations
                agg_metadata = bus_df.agg(
                    count('*').alias('total_updates'),
                    min(col('location.latitude')).alias('min_latitude'),
                    max(col('location.latitude')).alias('max_latitude'),
                    min(col('location.longitude')).alias('min_longitude'),
                    max(col('location.longitude')).alias('max_longitude')
                ).collect()[0]

                # Combine aggregated data with existing metadata
                bus_metadata = {
                    'bus_id': bus_id,
                    'start_time': self.metadata['session_data']['start_time'],
                    'end_time': self.metadata['session_data']['end_time'],
                    'duration_seconds': duration,
                    'total_updates': agg_metadata['total_updates'],
                    'latitude_range': {
                        'min': agg_metadata['min_latitude'],
                        'max': agg_metadata['max_latitude']
                    },
                    'longitude_range': {
                        'min': agg_metadata['min_longitude'],
                        'max': agg_metadata['max_longitude']
                    },
                    'updates': updates
                }
                
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                filename = f"{PROCESSED_DATA_DIR}/bus_metadata_{bus_id}_{timestamp}.json"
                
                self.hdfs_manager.write_json_data(filename, bus_metadata)
                self.logger.info(f"Saved metadata for bus {bus_id} to HDFS: {filename}")

        finally:
            spark.stop()

    async def run(self):
        try:
            self.logger.info("Starting B52 bus tracking with enhanced metadata storage")
            self.logger.info("Press Ctrl+C to stop")
            save_metadata_flag = True

            async with aiohttp.ClientSession() as session:
                while self.running:
                    try:
                        vehicles = await BusDataFetcher.fetch_data(session, self.logger)

                        if vehicles:
                            self.logger.info(f"Processing {len(vehicles)} buses at {datetime.now().strftime('%I:%M:%S %p')}")

                            for vehicle in vehicles:
                                bus_id, geojson_data = self.process_vehicle_data(vehicle)
                                if bus_id and geojson_data:
                                    self.hdfs_manager.write_bus_data(bus_id, geojson_data)

                            self.logger.info(f"Next update in {UPDATE_INTERVAL} seconds...")

                            if save_metadata_flag:
                                self.save_session_metadata()
                                save_metadata_flag = False

                        else:
                            self.logger.warning("No vehicles found in the current update")

                        await asyncio.sleep(UPDATE_INTERVAL)

                    except asyncio.CancelledError:
                        break
                    except Exception as e:
                        self.logger.error(f"Error in main loop: {str(e)}")
                        self.logger.error(traceback.format_exc())
                        await asyncio.sleep(UPDATE_INTERVAL)

        except Exception as e:
            self.logger.error(f"Fatal error in the run method: {str(e)}")
            self.logger.error(traceback.format_exc())

async def main():
    tracker = BusTracker()
    await tracker.run()

if __name__ == "__main__":
    asyncio.run(main())