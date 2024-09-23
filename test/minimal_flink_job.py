import os
import logging
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.common.serialization import SimpleStringSchema

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def minimal_job():
    env = StreamExecutionEnvironment.get_execution_environment()

    # Add Kafka connector dependencies
    current_dir = os.path.dirname(os.path.abspath(__file__))
    kafka_jar = os.path.join(current_dir, 'flink-connector-kafka-1.17.1.jar')
    kafka_client_jar = os.path.join(current_dir, 'kafka-clients-2.8.0.jar')
    
    env.add_jars(f"file://{kafka_jar}", f"file://{kafka_client_jar}")

    # Kafka consumer
    kafka_consumer = FlinkKafkaConsumer(
        topics='iot_data',
        deserialization_schema=SimpleStringSchema(),
        properties={'bootstrap.servers': 'kafka:9092', 'group.id': 'flink-iot-processor'}
    )

    # Kafka producer
    kafka_producer = FlinkKafkaProducer(
        topic='processed_iot_data',
        serialization_schema=SimpleStringSchema(),
        producer_config={'bootstrap.servers': 'kafka:9092'}
    )

    # Create a simple pipeline
    env.add_source(kafka_consumer) \
       .map(lambda x: x.upper()) \
       .add_sink(kafka_producer)

    # Execute
    env.execute("Minimal Flink Job")

if __name__ == "__main__":
    try:
        minimal_job()
    except Exception as e:
        logger.error(f"Job failed: {str(e)}", exc_info=True)