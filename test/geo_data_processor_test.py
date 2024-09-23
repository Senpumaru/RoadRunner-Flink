from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.typeinfo import Types
from pyflink.datastream.functions import MapFunction
import json
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class GeoDataProcessor(MapFunction):
    def map(self, value):
        try:
            data = json.loads(value)
            lat, lon = data['latitude'], data['longitude']
            distance = (lat ** 2 + lon ** 2) ** 0.5
            result = json.dumps({
                'original': data,
                'distance_from_origin': distance
            })
            logger.info(f"Processed result: {result}")
            return result
        except Exception as e:
            logger.error(f"Error processing event: {str(e)}", exc_info=True)
            return json.dumps({'error': str(e), 'original': value})

def geo_data_job():
    env = StreamExecutionEnvironment.get_execution_environment()

    # Create a simple source
    data_stream = env.from_collection(
        collection=[
            '{"latitude": 40.7128, "longitude": -74.0060}',
            '{"latitude": 34.0522, "longitude": -118.2437}',
            '{"latitude": 51.5074, "longitude": -0.1278}'
        ],
        type_info=Types.STRING()
    )

    processed_stream = data_stream.map(GeoDataProcessor(), output_type=Types.STRING())

    processed_stream.print()

    env.execute("Simplified Geo Data Processor Job")

if __name__ == "__main__":
    geo_data_job()