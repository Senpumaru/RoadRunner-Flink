import os
import json
import logging
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream.functions import MapFunction
from pyflink.common import Configuration

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class IoTDataProcessor(MapFunction):
    def __init__(self):
        self.temperature_sum = 0
        self.temperature_count = 0
        self.temperature_sq_sum = 0

    def map(self, event):
        try:
            data = json.loads(event)
            temperature = float(data['temperature'])

            # Update running statistics
            self.temperature_count += 1
            self.temperature_sum += temperature
            self.temperature_sq_sum += temperature ** 2

            # Calculate mean and standard deviation
            mean = self.temperature_sum / self.temperature_count
            variance = (self.temperature_sq_sum / self.temperature_count) - (mean ** 2)
            std_dev = variance ** 0.5

            # Check if the temperature is an outlier (more than 3 standard deviations from the mean)
            is_outlier = abs(temperature - mean) > 3 * std_dev

            # If it's not an outlier, return the data; otherwise, return None
            if not is_outlier:
                return json.dumps({
                    'device_id': str(data['device_id']),
                    'timestamp': str(data['timestamp']),
                    'temperature': temperature,
                    'humidity': float(data['humidity']),
                    'pressure': float(data['pressure']),
                    'battery_level': float(data['battery_level'])
                })
            else:
                logger.info(f"Outlier detected: {data}")
                return None

        except Exception as e:
            logger.error(f"Error processing event: {str(e)}", exc_info=True)
            return None

def iot_data_job():
    try:
        logger.info("Starting IoT data processing job")
        
        # Set up the execution environment
        config = Configuration()
        config.set_string("rest.address", "jobmanager")
        config.set_integer("rest.port", 8081)
        env = StreamExecutionEnvironment.get_execution_environment(config)

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

        # Read from Kafka, process the stream, and write back to Kafka
        env.add_source(kafka_consumer) \
           .map(IoTDataProcessor(), output_type=Types.STRING()) \
           .filter(lambda x: x is not None) \
           .add_sink(kafka_producer)

        logger.info("IoT data processing pipeline set up")

        # Execute the Flink job
        env.execute("IoT Data Outlier Detector Job")

    except Exception as e:
        logger.error(f"Error in iot_data_job: {str(e)}", exc_info=True)

if __name__ == "__main__":
    iot_data_job()