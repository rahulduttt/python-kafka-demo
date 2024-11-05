from kafka import KafkaProducer, KafkaConsumer
import json
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Simple test producer
def test_producer():
    try:
        # Create producer instance
        producer = KafkaProducer(
            bootstrap_servers=['localhost:29092'],
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
        )
        
        # Send test message
        message = {"test": "Hello Kafka!"}
        producer.send('test-topic', value=message)
        producer.flush()
        logger.info(f"Successfully sent message: {message}")
        
    except Exception as e:
        logger.error(f"Error producing message: {str(e)}")
    finally:
        producer.close()

# Simple test consumer
def test_consumer():
    try:
        # Create consumer instance
        consumer = KafkaConsumer(
            'test-topic',
            bootstrap_servers=['localhost:29092'],
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='test-group',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        
        # Consume messages
        logger.info("Waiting for messages...")
        for message in consumer:
            logger.info(f"Received message: {message.value}")
            break  # Exit after first message
            
    except Exception as e:
        logger.error(f"Error consuming message: {str(e)}")
    finally:
        consumer.close()

if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        if sys.argv[1] == "--produce":
            test_producer()
        elif sys.argv[1] == "--consume":
            test_consumer()
    else:
        print("Usage: python kafka_test.py --produce|--consume")