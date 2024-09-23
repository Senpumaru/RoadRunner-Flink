import os
import sys
import json
import logging
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream.functions import MapFunction, WindowFunction
from pyflink.datastream.window import TumblingProcessingTimeWindows
from pyflink.common.time import Time
from pyflink.common import Configuration

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class IoTDataParser(MapFunction):
    def map(self, event):
        try:
            data = json.loads(event)
            return (
                str(data['device_id']),
                str(data['timestamp']),
                float(data['temperature']),
                float(data['humidity']),
                float(data['pressure']),
                float(data['battery_level'])
            )
        except Exception as e:
            logger.error(f"Error parsing event: {str(e)}", exc_info=True)
            return None

class OutlierDetector(WindowFunction):
    def apply(self, key, window, inputs, out):
        temperatures = [x[2] for x in inputs if x is not None]
        if temperatures:
            mean = sum(temperatures) / len(temperatures)
            std_dev = (sum((x - mean) ** 2 for x in temperatures) / len(temperatures)) ** 0.5
            for record in inputs:
                if record is not None:
                    device_id, timestamp, temperature, humidity, pressure, battery_level = record
                    is_outlier = abs(temperature - mean) > 3 * std_dev
                    out.collect(json.dumps({
                        'device_id': device_id,
                        'timestamp': str(timestamp),  # Convert to string
                        'temperature': float(temperature),  # Ensure it's a float
                        'humidity': float(humidity),
                        'pressure': float(pressure),
                        'battery_level': float(battery_level),
                        'is_outlier': bool(is_outlier)  # Ensure it's a boolean
                    }))
                    if is_outlier:
                        logger.info(f"Outlier detected: {record}")

def iot_data_job():
    try:
        logger.info("Starting IoT data processing job")
        # Set up the execution environment
        config = Configuration()
        config.set_string("rest.address", "jobmanager")
        config.set_integer("rest.port", 8081)
        env = StreamExecutionEnvironment.get_execution_environment(config)

        # Add Kafka connector and client dependencies
        current_dir = os.path.dirname(os.path.abspath(__file__))
        kafka_jar = os.path.join(current_dir, 'flink-connector-kafka-1.17.1.jar')
        kafka_client_jar = os.path.join(current_dir, 'kafka-clients-2.8.0.jar')
        
        if not os.path.exists(kafka_jar):
            raise FileNotFoundError(f"Kafka connector JAR not found: {kafka_jar}")
        if not os.path.exists(kafka_client_jar):
            raise FileNotFoundError(f"Kafka client JAR not found: {kafka_client_jar}")
        
        env.add_jars(f"file://{kafka_jar}", f"file://{kafka_client_jar}")
        logger.info(f"Added JAR files: {kafka_jar}, {kafka_client_jar}")

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
        logger.info("Configured Kafka consumer")

        # Kafka producer configuration
        kafka_producer = FlinkKafkaProducer(
            topic='processed_iot_data',
            serialization_schema=SimpleStringSchema(),
            producer_config={
                'bootstrap.servers': 'kafka:9092',
                'transaction.timeout.ms': '5000'
            }
        )
        logger.info("Configured Kafka producer")

        # Read from Kafka
        stream = env.add_source(kafka_consumer)
        logger.info("Added Kafka source to the stream")

        # Process the stream
        processed_stream = stream \
            .map(IoTDataParser(), output_type=Types.TUPLE([Types.STRING(), Types.STRING(), Types.FLOAT(), Types.FLOAT(), Types.FLOAT(), Types.FLOAT()])) \
            .filter(lambda x: x is not None) \
            .key_by(lambda x: x[0]) \
            .window(TumblingProcessingTimeWindows.of(Time.minutes(5))) \
            .apply(OutlierDetector(), output_type=Types.STRING())

        logger.info("Added IoT data processing to the stream")

        # Write to Kafka
        processed_stream.add_sink(kafka_producer)
        logger.info("Added Kafka sink to the stream")

        # Execute the Flink job
        logger.info("Executing Flink job")
        env.execute("IoT Data Outlier Detector Job")
    except Exception as e:
        logger.error(f"Error in iot_data_job: {str(e)}", exc_info=True)

if __name__ == "__main__":
    iot_data_job()