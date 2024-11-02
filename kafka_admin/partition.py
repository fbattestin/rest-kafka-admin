from confluent_kafka import KafkaException
from confluent_kafka.admin import AdminClient, NewPartitions
import logging


logging.basicConfig()


def create_partitions(a: AdminClient, topics: list[str]) -> dict:
    """
    Create additional partitions for the given topics.

    Args:
        a (AdminClient): The Kafka AdminClient instance.
        topics (list[str]): A list of topic names and their new partition counts.
                            The list should alternate between topic name and partition count.
                            Example: ['topic1', '3', 'topic2', '5'].

    Returns:
        dict: A dictionary with the status and message of the operation.
    """
    logger = logging.getLogger(__name__)
    logger.info("Starting partition creation process.")

    new_parts = [NewPartitions(topic, int(new_total_count)) for
                 topic, new_total_count in zip(topics[0::2], topics[1::2])]

    fs = a.create_partitions(new_parts, validate_only=False)

    for topic, f in fs.items():
        try:
            f.result() 
            logger.info(f"Additional partitions created for topic {topic}")
        except Exception as e:
            logger.error(f"Failed to add partitions to topic {topic}: {e}")
            raise KafkaException(f"Failed to add partitions to topic {topic}: {e}")

    return {'status': 'success', 'message': f"Additional partitions created for topic {topic}"}
