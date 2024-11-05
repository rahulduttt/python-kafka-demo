from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError
import json
import logging
from typing import Dict, Any
from datetime import datetime
import sys

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class KafkaClient:
    def __init__(
        self, 
        bootstrap_servers: str = 'localhost:29092',
        topic: str = 'test-topic'
    ):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic

    def create_producer(self) -> KafkaProducer:
        """Create and return a Kafka producer instance."""
        try:
            return KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                # Enable idempotence to ensure exactly-once delivery
                acks='all',
                retries=3,
                enable_idempotence=True
            )
        except Exception as e:
            logger.error(f"Failed to create producer: {str(e)}")
            raise

    def create_consumer(self, group_id: str) -> KafkaConsumer:
        """Create and return a Kafka consumer instance."""
        try:
            return KafkaConsumer(
                self.topic,
                bootstrap_servers=self.bootstrap_servers,
                group_id=group_id,
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                value_deserializer=lambda x: json.loads(x.decode('utf-8'))
            )
        except Exception as e:
            logger.error(f"Failed to create consumer: {str(e)}")
            raise

    def produce_message(self, message: Dict[str, Any]) -> bool:
        """
        Produce a message to Kafka topic.
        Returns True if successful, False otherwise.
        """
        producer = self.create_producer()
        try:
            # Add timestamp to message
            message['timestamp'] = datetime.now().isoformat()
            
            # Produce message
            future = producer.send(self.topic, message)
            # Wait for message to be delivered
            record_metadata = future.get(timeout=10)
            
            logger.info(
                f"Message sent to topic={record_metadata.topic} "
                f"partition={record_metadata.partition} "
                f"offset={record_metadata.offset}"
            )
            return True
            
        except KafkaError as e:
            logger.error(f"Failed to send message: {str(e)}")
            return False
        finally:
            producer.close()

    def consume_messages(self, group_id: str, timeout_ms: int = 1000):
        """
        Consume messages from Kafka topic.
        timeout_ms: how long to wait for new messages in milliseconds
        """
        consumer = self.create_consumer(group_id)
        try:
            while True:
                messages = consumer.poll(timeout_ms=timeout_ms)
                for topic_partition, records in messages.items():
                    for record in records:
                        yield {
                            'topic': record.topic,
                            'partition': record.partition,
                            'offset': record.offset,
                            'value': record.value,
                            'timestamp': record.timestamp
                        }
        except KeyboardInterrupt:
            logger.info("Stopping consumer...")
        finally:
            consumer.close()

def example_usage():
    # Create Kafka client instance
    kafka_client = KafkaClient(
        bootstrap_servers='localhost:29092',
        topic='test-topic'
    )

    # Example: Produce some messages
    messages = [
        {'user_id': 1, 'action': 'login', 'platform': 'web'},
        {'user_id': 2, 'action': 'purchase', 'item_id': 'SKU123'},
        {'user_id': 1, 'action': 'logout', 'platform': 'web'}
    ]

    for msg in messages:
        success = kafka_client.produce_message(msg)
        if success:
            print(f"Successfully produced message: {msg}")
        else:
            print(f"Failed to produce message: {msg}")

    # Example: Consume messages
    print("\nConsuming messages:")
    for message in kafka_client.consume_messages(group_id='test-group'):
        print(f"Received message: {message}")
        # Break after consuming all test messages
        if message['value']['action'] == 'logout':
            break

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == '--example':
        example_usage()
    else:
        print("Usage: python script.py --example")