from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import MapFunction
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TestMapFunction(MapFunction):
    def map(self, value):
        logger.info(f"Processing value: {value}")
        return "Processed: " + value

def test_job():
    try:
        env = StreamExecutionEnvironment.get_execution_environment()
        
        # Add these lines to make sure the Python environment is correctly set
        env.add_python_file("/opt/venv/lib/python3.10/site-packages/pyflink/fn_execution/boot.py")
        env.set_python_executable("/opt/venv/bin/python")
        
        logger.info("Creating data stream")
        data_stream = env.from_collection(["Hello", "Flink", "Python", "UDF"])

        logger.info("Applying TestMapFunction")
        result = data_stream.map(TestMapFunction())

        logger.info("Setting up result sink")
        result.print()

        logger.info("Executing Flink job")
        env.execute("Test Flink Python UDF Job")
    except Exception as e:
        logger.error(f"Error in Flink job: {str(e)}", exc_info=True)
        raise

if __name__ == "__main__":
    test_job()