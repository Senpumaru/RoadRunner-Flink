from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import MapFunction
from pyflink.common import Configuration

class TestMapFunction(MapFunction):
    def map(self, value):
        return "Processed: " + value

def test_job():
    config = Configuration()
    config.set_string("rest.address", "jobmanager")
    config.set_integer("rest.port", 8081)

    env = StreamExecutionEnvironment.get_execution_environment(config)
    
    # Create a source
    data_stream = env.from_collection(["Hello", "Flink", "Python", "UDF"])

    # Apply the TestMapFunction
    result = data_stream.map(TestMapFunction())

    # Print the result
    result.print()

    # Execute the job
    env.execute("Test Flink Python UDF Job")

if __name__ == "__main__":
    test_job()