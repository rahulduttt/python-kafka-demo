# Dockerized Apache Kafka Setup Guide

This guide provides a complete setup for running Apache Kafka using Docker containers.
We'll use Docker Compose to manage Kafka and ZooKeeper containers.

## Project Structure
```
python-kafka-demo/
├── docker-compose.yml
├── .env
└── README.md
```

## Setup Instructions

1. Start the containers:
```bash
docker-compose up -d
```

2. Verify the setup:
```bash
# Check container status
docker-compose ps

# Check Kafka logs
docker-compose logs -f kafka

# Check ZooKeeper logs
docker-compose logs -f zookeeper
```

## Testing the Setup

1. Create a topic:
```bash
docker-compose exec kafka kafka-topics --create --topic test-topic \
    --partitions 1 --replication-factor 1 --bootstrap-server localhost:9092
```

2. List topics:
```bash
docker-compose exec kafka kafka-topics --list --bootstrap-server localhost:9092
```

3. Start a console producer:
```bash
docker-compose exec kafka kafka-console-producer --topic test-topic \
    --bootstrap-server localhost:9092
```

4. Start a console consumer (in a new terminal):
```bash
docker-compose exec kafka kafka-console-consumer --topic test-topic \
    --from-beginning --bootstrap-server localhost:9092
```

## Cleanup

To stop and remove the containers:
```bash
docker-compose down
```

To stop and remove containers along with volumes:
```bash
docker-compose down -v
```

## Common Issues and Troubleshooting

1. **Connection Refused**: If you can't connect to Kafka, ensure:
   - All containers are running (`docker-compose ps`)
   - The ports are correctly mapped (`docker-compose port kafka 9092`)
   - No firewall issues are blocking the connections

2. **Topic Creation Fails**: Check:
   - ZooKeeper connection (`docker-compose logs zookeeper`)
   - Kafka broker logs (`docker-compose logs kafka`)

3. **Performance Issues**: Consider adjusting:
   - JVM heap size in the Docker Compose file
   - Number of partitions for topics
   - Replication factor settings

## Using the Script
You will need to install the following packages
```bash
pip install kafka-python
```

You can run the script by running the following command:
```bash
python script.py --example
```