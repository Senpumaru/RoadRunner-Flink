import os
import logging
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.functions import MapFunction
from pyflink.common.typeinfo import Types
from pyflink.common import Configuration

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DebugMap(MapFunction):
    def map(self, value):
        logger.info(f"Processing message: {value}")
        return value

def debug_job():
    # Set up the execution environment
    config = Configuration()
    config.set_string("rest.address", "jobmanager")
    config.set_integer("rest.port", 8081)
    env = StreamExecutionEnvironment.get_execution_environment(config)

    # Add Kafka connector dependencies
    current_dir = os.path.dirname(os.path.abspath(__file__))
    kafka_jar = os.path.join(current_dir, 'flink-connector-kafka-1.17.1.jar')
    kafka_client_jar = os.path.join(current_dir, 'kafka-clients-2.8.0.jar')
    
    if not os.path.exists(kafka_jar):
        raise FileNotFoundError(f"Kafka connector JAR not found: {kafka_jar}")
    if not os.path.exists(kafka_client_jar):
        raise FileNotFoundError(f"Kafka client JAR not found: {kafka_client_jar}")
    
    env.add_jars(f"file://{kafka_jar}", f"file://{kafka_client_jar}")

    # Kafka consumer configuration
    kafka_consumer = FlinkKafkaConsumer(
        topics='iot_data',
        deserialization_schema=SimpleStringSchema(),
        properties={
            'bootstrap.servers': 'kafka:9092',
            'group.id': 'flink-iot-processor',
            'auto.offset.reset': 'earliest'
        }
    )

    # Kafka producer configuration
    kafka_producer = FlinkKafkaProducer(
        topic='processed_iot_data',
        serialization_schema=SimpleStringSchema(),
        producer_config={
            'bootstrap.servers': 'kafka:9092',
            'transaction.timeout.ms': '5000'
        }
    )

    # Create the data stream
    try:
        stream = env.add_source(kafka_consumer)
        logger.info("Kafka consumer added to the stream")
    except Exception as e:
        logger.error(f"Error adding Kafka consumer: {str(e)}")
        raise

    # Process the stream
    try:
        processed_stream = stream.map(DebugMap(), output_type=Types.STRING())
        logger.info("DebugMap added to the stream")
    except Exception as e:
        logger.error(f"Error adding DebugMap: {str(e)}")
        raise

    # Add the sink
    try:
        processed_stream.add_sink(kafka_producer)
        logger.info("Kafka producer added as sink")
    except Exception as e:
        logger.error(f"Error adding Kafka producer: {str(e)}")
        raise

    # Execute the job
    logger.info("Starting Flink job")
    env.execute("Debug Kafka to Kafka Job")

if __name__ == "__main__":
    try:
        debug_job()
    except Exception as e:
        logger.error(f"Job failed: {str(e)}", exc_info=True)