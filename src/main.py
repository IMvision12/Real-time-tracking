import asyncio
from bus_tracker import BusTracker
from config import setup_logging
import traceback

async def main():
    logger = setup_logging()
    try:
        tracker = BusTracker()
        await tracker.run()
    except Exception as e:
        logger.error(f"Failed to start bus tracker: {str(e)}")
        logger.error(f"Stack trace: {traceback.format_exc()}")

if __name__ == "__main__":
    asyncio.run(main())