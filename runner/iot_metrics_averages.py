import os
import json
import logging
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream.functions import MapFunction, ReduceFunction
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
                data['device_id'],
                (data['temperature'], 1),
                (data['humidity'], 1),
                (data['pressure'], 1),
                (data['battery_level'], 1)
            )
        except Exception as e:
            logger.error(f"Error parsing event: {str(e)}", exc_info=True)
            return None

class MetricsAggregator(ReduceFunction):
    def reduce(self, value1, value2):
        return (
            value1[0],  # device_id
            (value1[1][0] + value2[1][0], value1[1][1] + value2[1][1]),  # temperature
            (value1[2][0] + value2[2][0], value1[2][1] + value2[2][1]),  # humidity
            (value1[3][0] + value2[3][0], value1[3][1] + value2[3][1]),  # pressure
            (value1[4][0] + value2[4][0], value1[4][1] + value2[4][1])   # battery_level
        )

class AverageCalculator(MapFunction):
    def map(self, value):
        device_id, temp, humidity, pressure, battery = value
        return json.dumps({
            'device_id': device_id,
            'avg_temperature': temp[0] / temp[1],
            'avg_humidity': humidity[0] / humidity[1],
            'avg_pressure': pressure[0] / pressure[1],
            'avg_battery_level': battery[0] / battery[1]
        })

def iot_metrics_averages_job():
    try:
        logger.info("Starting IoT metrics averages job")
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
                'group.id': 'flink-iot-averages-processor',
                'auto.offset.reset': 'earliest'
            }
        )
        logger.info("Configured Kafka consumer")

        # Kafka producer configuration
        kafka_producer = FlinkKafkaProducer(
            topic='iot_metrics_averages',
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
            .map(IoTDataParser(), output_type=Types.TUPLE([Types.STRING(), Types.TUPLE([Types.DOUBLE(), Types.INT()]), Types.TUPLE([Types.DOUBLE(), Types.INT()]), Types.TUPLE([Types.DOUBLE(), Types.INT()]), Types.TUPLE([Types.DOUBLE(), Types.INT()])])) \
            .filter(lambda x: x is not None) \
            .key_by(lambda x: x[0]) \
            .window(TumblingProcessingTimeWindows.of(Time.seconds(10))) \
            .reduce(MetricsAggregator()) \
            .map(AverageCalculator(), output_type=Types.STRING())

        logger.info("Added IoT metrics processing to the stream")

        # Write to Kafka
        processed_stream.add_sink(kafka_producer)
        logger.info("Added Kafka sink to the stream")

        # Execute the Flink job
        logger.info("Executing Flink job")
        env.execute("IoT Metrics 10-Second Averages Job")
    except Exception as e:
        logger.error(f"Error in iot_metrics_averages_job: {str(e)}", exc_info=True)

if __name__ == "__main__":
    iot_metrics_averages_job()