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
            logger.info(f"Processing event: {event}")
            data = json.loads(event)
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
        logger.info("Starting geo_data_job")
        env = StreamExecutionEnvironment.get_execution_environment()

        # Set parallelism to 2
        env.set_parallelism(2)

        logger.info(f"Python executable: {sys.executable}")
        logger.info(f"Python version: {sys.version}")
        logger.info(f"PYTHONPATH: {os.environ.get('PYTHONPATH')}")

        # Add Kafka connector JAR
        env.add_jars("file:///opt/flink/lib/flink-connector-kafka-1.17.1.jar")
        logger.info("Added Kafka connector JAR")

        # Kafka consumer configuration
        kafka_consumer = FlinkKafkaConsumer(
            topics='geo_data',
            deserialization_schema=SimpleStringSchema(),
            properties={'bootstrap.servers': 'kafka:9092', 'group.id': 'flink-geo-processor'}
        )
        logger.info("Configured Kafka consumer")

        # Kafka producer configuration for the output topic
        kafka_producer = FlinkKafkaProducer(
            topic='processed_geo_data',
            serialization_schema=SimpleStringSchema(),
            producer_config={'bootstrap.servers': 'kafka:9092'}
        )
        logger.info("Configured Kafka producer for processed_geo_data topic")

        # Kafka producer for filtered data
        filtered_producer = FlinkKafkaProducer(
            topic='filtered_geo_data',
            serialization_schema=SimpleStringSchema(),
            producer_config={'bootstrap.servers': 'kafka:9092'}
        )
        logger.info("Configured Kafka producer for filtered_geo_data topic")

        # Read from Kafka
        stream = env.add_source(kafka_consumer)
        logger.info("Added Kafka source to the stream")

        # Process the stream
        processed_stream = stream.map(GeoDataProcessor(), output_type=Types.STRING())
        logger.info("Added GeoDataProcessor to the stream")

        # Write processed data to Kafka
        processed_stream.add_sink(kafka_producer)
        logger.info("Added Kafka sink for processed data")

        # Filter and write to another topic
        filtered_stream = processed_stream.filter(DistanceFilter(100))
        filtered_stream.add_sink(filtered_producer)
        logger.info("Added filter and sink for filtered data")

        # Execute the Flink job
        logger.info("Executing Flink job")
        env.execute("Geo Data Processor Job")
    except Exception as e:
        logger.error(f"Error in geo_data_job: {str(e)}", exc_info=True)

if __name__ == "__main__":
    geo_data_job()