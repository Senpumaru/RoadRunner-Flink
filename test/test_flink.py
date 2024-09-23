from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common import Configuration

def main():
    config = Configuration()
    config.set_string("rest.address", "jobmanager")
    config.set_integer("rest.port", 8081)

    env = StreamExecutionEnvironment.get_execution_environment(config)

    text = env.from_collection(["Hello", "Flink", "Test"])

    text.print()

    env.execute("Test PyFlink Job")

if __name__ == "__main__":
    main()