import os
import sys
import json
import logging
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream.functions import MapFunction, FilterFunction

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class GeoDataProcessor(MapFunction):
    def map(self, event):
        try:
            data = json.loads(event)
            lat, lon = data['latitude'], data['longitude']
            distance = (lat ** 2 + lon ** 2) ** 0.5
            result = json.dumps({
                'original': data,
                'distance_from_origin': distance
            })
            return result
        except Exception as e:
            logger.error(f"Error processing event: {str(e)}", exc_info=True)
            return json.dumps({'error': str(e), 'original': event})

class DistanceFilter(FilterFunction):
    def __init__(self, threshold):
        self.threshold = threshold

    def filter(self, value):
        try:
            data = json.loads(value)
            return data['distance_from_origin'] > self.threshold
        except Exception as e:
            logger.error(f"Error in filter: {str(e)}", exc_info=True)
            return False

def geo_data_job():
    try:
        env = StreamExecutionEnvironment.get_execution_environment()

        # Set parallelism to 1 to reduce resource usage
        env.set_parallelism(1)

        # Add Kafka connector JAR
        env.add_jars("file:///opt/flink/lib/flink-connector-kafka-1.17.1.jar")

        # Kafka consumer configuration
        kafka_consumer = FlinkKafkaConsumer(
            topics='geo_data',
            deserialization_schema=SimpleStringSchema(),
            properties={'bootstrap.servers': 'kafka:9092', 'group.id': 'flink-geo-processor'}
        )

        # Kafka producer configuration for the output topic
        kafka_producer = FlinkKafkaProducer(
            topic='processed_geo_data',
            serialization_schema=SimpleStringSchema(),
            producer_config={'bootstrap.servers': 'kafka:9092'}
        )

        # Read from Kafka
        stream = env.add_source(kafka_consumer)

        # Process the stream
        processed_stream = stream.map(GeoDataProcessor(), output_type=Types.STRING())

        # Write processed data to Kafka
        processed_stream.add_sink(kafka_producer)

        # Filter and write to another topic (commented out for now)
        # filtered_stream = processed_stream.filter(DistanceFilter(100))
        # filtered_stream.add_sink(filtered_producer)

        # Execute the Flink job
        env.execute("Geo Data Processor Job V3 - Test")
    except Exception as e:
        logger.error(f"Error in geo_data_job: {str(e)}", exc_info=True)

if __name__ == "__main__":
    geo_data_job()