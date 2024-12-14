import aiohttp
import logging
from aiohttp import ClientTimeout
from config import *
import asyncio

class BusDataFetcher:
    @staticmethod
    async def fetch_data(session, logger):
        base_url = "http://bustime.mta.info/api/siri/vehicle-monitoring.json"
        url = f"{base_url}?key={API_KEY}&VehicleMonitoringDetailLevel=calls&LineRef={BUS_LINE}"
        
        timeout = ClientTimeout(total=10)
        try:
            async with session.get(url, timeout=timeout) as response:
                if response.status == 200:
                    data = await response.json()
                    vehicles = BusDataFetcher._get_vehicle_activities(data, logger)
                    logger.info(f"Successfully fetched data for {len(vehicles)} vehicles")
                    return vehicles
                else:
                    logger.error(f"Error fetching data: Status {response.status}")
                    return []
        except asyncio.TimeoutError:
            logger.error("Request timed out while fetching bus data")
            return []
        except Exception as e:
            logger.error(f"Error fetching data: {str(e)}")
            return []
    
    @staticmethod
    def _get_vehicle_activities(data, logger):
        try:
            return data['Siri']['ServiceDelivery']['VehicleMonitoringDelivery'][0]['VehicleActivity']
        except (KeyError, TypeError) as e:
            logger.error(f"Error parsing vehicle activities: {str(e)}")
            return []