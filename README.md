# Apache Flink for RoadRunner Project

This repository contains the configuration and setup for Apache Flink as part of the RoadRunner project. Flink is used for stream processing in conjunction with Apache Kafka.

## Prerequisites

- Docker and Docker Compose
- Access to the RoadRunner Kafka cluster

## Setup

1. Clone this repository:
   ```
   git clone https://github.com/your-org/roadrunner-flink.git
   cd roadrunner-flink
   ```

2. Ensure that the Kafka cluster is running and accessible from the `roadrunner-network`.

3. Start the Flink cluster:
   ```
   docker compose up -d --build
   ```

4. Access the Flink Web UI at `http://localhost:8085`.

## Project Structure

- `docker-compose.yml`: Defines the Flink cluster setup.
- `flink-checkpoints/`: Directory for storing Flink checkpoints.

## Usage

To submit a Flink job:

1. Build your Flink job JAR file.
2. Use the Flink CLI to submit the job:
   ```
   docker exec -it roadrunner-flink-jobmanager-1 flink run -d /path/to/your-job.jar
   ```

## Development

For local development and testing:

1. Set up a Python environment:
   ```
   python -m venv venv
   source venv/bin/activate
   pip install apache-flink
   ```

2. Create Flink programs using the Python API.

3. For Java development, use Maven or Gradle to manage dependencies and build your Flink jobs.

## Testing

To test Flink with Kafka:

1. Ensure Kafka is running and accessible.
2. Create a test topic in Kafka.
3. Develop a simple Flink job that reads from and writes to Kafka.
4. Submit the job and verify the results.

## Monitoring

Monitor your Flink jobs using the Flink Web UI at `http://localhost:8085`.

## Troubleshooting

- If you can't connect to Kafka, ensure that both Flink and Kafka are on the same Docker network (`roadrunner-network`).
- Check Flink logs: `docker logs roadrunner-flink-jobmanager-1`

## Contributing

Please read CONTRIBUTING.md for details on our code of conduct and the process for submitting pull requests.

## License

This project is licensed under the Apache License 2.0 - see the LICENSE file for details.