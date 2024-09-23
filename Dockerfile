FROM flink:1.17.1

# Install JDK
RUN apt-get update -y
RUN apt-get install -y openjdk-11-jdk

# Install python3 and pip3

RUN apt-get install -y python3 python3-pip
RUN pip3 install apache-flink==1.17.1
RUN ln -s /usr/bin/python3 /usr/bin/python

# Configure PATHS

# RUN export PYTHONPATH=$PYTHONPATH:/usr/local/lib/python3.*/dist-packages
RUN export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

# Set up working directory
WORKDIR /opt/flink/lib

# Download and copy necessary JAR files
RUN wget https://repo1.maven.org/maven2/org/apache/flink/flink-connector-kafka/1.17.1/flink-connector-kafka-1.17.1.jar && \
    wget https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/2.8.0/kafka-clients-2.8.0.jar

# Copy custom configuration
COPY flink-conf.yaml /opt/flink/conf/flink-conf.yaml

# Set environment variables
ENV FLINK_HOME=/opt/flink
ENV PATH=$PATH:$FLINK_HOME/bin
ENV PYTHONPATH=$PYTHONPATH:$FLINK_HOME/lib

# Set up working directory
WORKDIR /app

# Copy any necessary files
COPY . .

# Command to keep the container running and tail the logs
CMD ["sh", "-c", "tail -f /opt/flink/log/* & tail -f /dev/null"]