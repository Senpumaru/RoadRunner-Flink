from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.typeinfo import Types
from pyflink.datastream.functions import MapFunction
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common import Configuration
import json
import math
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class IoTData:
    def __init__(self, device_id, timestamp, temperature, humidity, pressure, battery_level):
        self.device_id = device_id
        self.timestamp = timestamp
        self.temperature = temperature
        self.humidity = humidity
        self.pressure = pressure
        self.battery_level = battery_level

class EnrichedIoTData:
    def __init__(self, device_id, timestamp, temperature, humidity, pressure, battery_level, heat_index, dew_point):
        self.device_id = device_id
        self.timestamp = timestamp
        self.temperature = temperature
        self.humidity = humidity
        self.pressure = pressure
        self.battery_level = battery_level
        self.heat_index = heat_index
        self.dew_point = dew_point

class EnrichmentFunction(MapFunction):
    def map(self, value):
        heat_index = self.calculate_heat_index(value.temperature, value.humidity)
        dew_point = self.calculate_dew_point(value.temperature, value.humidity)
        return EnrichedIoTData(
            value.device_id,
            value.timestamp,
            value.temperature,
            value.humidity,
            value.pressure,
            value.battery_level,
            heat_index,
            dew_point
        )

    def calculate_heat_index(self, temperature, humidity):
        return -42.379 + 2.04901523 * temperature + 10.14333127 * humidity

    def calculate_dew_point(self, temperature, humidity):
        a = 17.271
        b = 237.7
        temp = (a * temperature) / (b + temperature) + math.log(humidity / 100)
        return (b * temp) / (a - temp)

def main():
    try:
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
            topic='iot_data_enriched',
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

        # Parse the JSON data and convert to IoTData objects
        parsed_stream = stream.map(
            lambda x: IoTData(**json.loads(x)),
            output_type=Types.PICKLED_BYTE_ARRAY()
        )

        # Apply the enrichment function
        enriched_stream = parsed_stream.map(EnrichmentFunction(), output_type=Types.PICKLED_BYTE_ARRAY())

        # Convert enriched data back to JSON string
        json_stream = enriched_stream.map(
            lambda x: json.dumps({
                'device_id': x.device_id,
                'timestamp': x.timestamp,
                'temperature': x.temperature,
                'humidity': x.humidity,
                'pressure': x.pressure,
                'battery_level': x.battery_level,
                'heat_index': x.heat_index,
                'dew_point': x.dew_point
            }),
            output_type=Types.STRING()
        )

        # Write to Kafka
        json_stream.add_sink(kafka_producer)
        logger.info("Added Kafka sink to the stream")

        # Execute the Flink job
        logger.info("Executing Flink job")
        env.execute("IoT Data Enrichment Job")
    except Exception as e:
        logger.error(f"Error in iot_data_enrichment_job: {str(e)}", exc_info=True)

if __name__ == "__main__":
    main()