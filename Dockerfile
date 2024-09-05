# Use Java as the base image
FROM openjdk:11-jdk

# Install Python and pip
RUN apt-get update && apt-get install -y python3 python3-pip python3-dev

# Create symlinks for python and pip
RUN ln -s /usr/bin/python3 /usr/bin/python

# Install Maven for Java development
RUN apt-get install -y maven

# Set up Python environment
RUN pip install --no-cache-dir --upgrade pip
RUN pip install --no-cache-dir apache-flink==1.17.1

# Install Flink
ENV FLINK_VERSION=1.17.1
ENV FLINK_HOME=/opt/flink
RUN wget -qO- https://archive.apache.org/dist/flink/flink-${FLINK_VERSION}/flink-${FLINK_VERSION}-bin-scala_2.12.tgz | tar -xzf - -C /opt/ && \
    ln -s /opt/flink-${FLINK_VERSION} ${FLINK_HOME}

# Download Kafka connector and client JARs
RUN wget https://repo1.maven.org/maven2/org/apache/flink/flink-connector-kafka/1.17.1/flink-connector-kafka-1.17.1.jar -P ${FLINK_HOME}/lib/ && \
    wget https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/2.8.1/kafka-clients-2.8.1.jar -P ${FLINK_HOME}/lib/

# Add Flink to PATH
ENV PATH=$PATH:${FLINK_HOME}/bin

# Install additional tools
RUN apt-get install -y git curl wget

# Set up working directory
WORKDIR /app

# Copy any necessary files
COPY . .

# Set environment variables
ENV JAVA_HOME /usr/local/openjdk-11
ENV PATH $PATH:$JAVA_HOME/bin
ENV PYTHONPATH "${PYTHONPATH}:/app"

# Set Flink configuration environment variables
ENV FLINK_JOB_MANAGER_HOST jobmanager
ENV FLINK_JOB_MANAGER_PORT 6123

# Update Flink configuration
RUN echo "jobmanager.rpc.address: ${FLINK_JOB_MANAGER_HOST}" >> ${FLINK_HOME}/conf/flink-conf.yaml && \
    echo "rest.address: ${FLINK_JOB_MANAGER_HOST}" >> ${FLINK_HOME}/conf/flink-conf.yaml && \
    echo "pipeline.jars: $(ls ${FLINK_HOME}/lib/flink-connector-kafka*.jar):$(ls ${FLINK_HOME}/lib/kafka-clients*.jar)" >> ${FLINK_HOME}/conf/flink-conf.yaml

# Ensure Python is in the PATH for Flink
RUN echo "env.python.executable: $(which python3)" >> ${FLINK_HOME}/conf/flink-conf.yaml

# Command to keep the container running
CMD ["tail", "-f", "/dev/null"]