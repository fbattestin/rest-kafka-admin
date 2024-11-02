from confluent_kafka.admin import AdminClient, NewTopic, KafkaException
from confluent_kafka import TopicCollection, TopicPartition, OffsetSpec, IsolationLevel
import logging


logging.basicConfig()

def create(a: AdminClient, topics: list[str]) -> dict[str, dict[str, str]]:
    """Create topics in the Kafka cluster.

    Args:
        a (AdminClient): The Kafka AdminClient instance.
        topics (list[str]): List of topic names to create.

    Returns:
        dict[str, dict[str, str]]: A dictionary with topic names as keys and a dictionary with 'status' and 'message' as values.
    """
    logging.info("Creating topics: %s", topics)
    new_topics = [NewTopic(topic, num_partitions=3, replication_factor=1) for topic in topics]
    fs = a.create_topics(new_topics)
    results = {}

    for topic, f in fs.items():
        try:
            f.result()
            logging.info("Topic %s created", topic)
            results[topic] = {'status': 'success', 'message': 'Topic created successfully'}
        except Exception as e:
            logging.error("Failed to create topic %s: %s", topic, e)
            raise RuntimeError(f"Failed to create topic {topic}: {e}")

    return results


def delete(a: AdminClient, topics: list[str]) -> dict[str, dict[str, str]]:
    """Delete topics from the Kafka cluster.

    Args:
        a (AdminClient): The Kafka AdminClient instance.
        topics (list[str]): List of topic names to delete.

    Returns:
        dict[str, dict[str, str]]: A dictionary with topic names as keys and a dictionary with 'status' and 'message' as values.
    """
    logging.info("Deleting topics: %s", topics)
    fs = a.delete_topics(topics, operation_timeout=30)
    results = {}

    for topic, f in fs.items():
        try:
            f.result()
            logging.info("Topic %s deleted", topic)
            results[topic] = {'status': 'success', 'message': 'Topic deleted successfully'}
        except Exception as e:
            logging.error("Failed to delete topic %s: %s", topic, e)
            raise RuntimeError(f"Failed to delete topic {topic}: {e}")

    return results

def describe_topics(a: AdminClient, args: list[str]) -> None:
    """
    Describe topics in the Kafka cluster.

    Args:
        a (AdminClient): The Kafka AdminClient instance.
        args (list[str]): List of arguments where the first element is a flag for including authorized operations and the rest are topic names.
    """
    logging.info("Describing topics with args: %s", args)
    include_auth_ops = bool(int(args[0]))
    args = args[1:]
    topics = TopicCollection(topic_names=args)
    results = {topic: {'partitions': []} for topic in args}
    futureMap = a.describe_topics(topics, request_timeout=10, include_authorized_operations=include_auth_ops)

    for topic_name, future in futureMap.items():
        try:
            t = future.result()
            logging.info("Topic name: %s", t.name)
            logging.info("Topic id: %s", t.topic_id)
            if t.is_internal:
                logging.info("Topic is Internal")

            if include_auth_ops:
                logging.info("Authorized operations:")
                op_string = "  ".join(acl_op.name for acl_op in t.authorized_operations)
                logging.info("    %s", op_string)

            logging.info("Partition Information")
            for partition in t.partitions:
                partition_info = {
                    'id': partition.id,
                    'leader': partition.leader,
                    'replicas': [replica for replica in partition.replicas],
                    'in_sync_replicas': [isr for isr in partition.isr]
                }
                results[topic_name]['partitions'].append(partition_info)

        except KafkaException as e:
            logging.error("Error while describing topic '%s': %s", topic_name, e)
        except Exception as e:
            logging.error("Unexpected error: %s", e)
            raise RuntimeError(f"Unexpected error: {e}")

    return {'status': 'success', 'message': results}



def list_offsets(a: AdminClient, args: list[str]) -> dict[str, dict[str, str]]:
    """
    List offsets for the given topics and partitions.

    Args:
        a (AdminClient): The Kafka AdminClient instance.
        args (list[str]): List of arguments where the first element is the isolation level and the rest are topic, partition, and offset spec.

    Returns:
        dict[str, dict[str, str]]: A dictionary with topic names as keys and a dictionary with partition information as values.
    """
    logging.info("Listing offsets with args: %s", args)
    if len(args) == 0:
        raise ValueError("Invalid number of arguments for list offsets, expected at least 1, got 0")

    isolation_level = IsolationLevel[args[0]]
    topic_partition_offsets = {}
    i = 1
    partition_i = 1

    while i < len(args):
        if i + 3 > len(args):
            raise ValueError(f"Invalid number of arguments for list offsets, partition {partition_i}, expected 3, got {len(args) - i}")

        topic = args[i]
        partition = int(args[i + 1])
        topic_partition = TopicPartition(topic, partition)

        if args[i + 2] == "EARLIEST":
            offset_spec = OffsetSpec.earliest()
        elif args[i + 2] == "LATEST":
            offset_spec = OffsetSpec.latest()
        elif args[i + 2] == "MAX_TIMESTAMP":
            offset_spec = OffsetSpec.max_timestamp()
        elif args[i + 2] == "TIMESTAMP":
            if i + 4 > len(args):
                raise ValueError(f"Invalid number of arguments for list offsets, partition {partition_i}, expected 4, got {len(args) - i}")
            offset_spec = OffsetSpec.for_timestamp(int(args[i + 3]))
            i += 1
        else:
            raise ValueError("Invalid OffsetSpec, must be EARLIEST, LATEST, MAX_TIMESTAMP or TIMESTAMP")

        topic_partition_offsets[topic_partition] = offset_spec
        i += 3
        partition_i += 1

    futmap = a.list_offsets(topic_partition_offsets, isolation_level=isolation_level, request_timeout=30)
    results = {}

    for partition, fut in futmap.items():
        try:
            result = fut.result()
            logging.info("Topic: %s, Partition: %d, Offset: %d, Timestamp: %d", partition.topic, partition.partition, result.offset, result.timestamp)
            results[partition.topic] = results.get(partition.topic, {})
            results[partition.topic][partition.partition] = {
                'offset': result.offset,
                'timestamp': result.timestamp
            }
        except KafkaException as e:
            logging.error("Error listing offsets for topic %s partition %d: %s", partition.topic, partition.partition, e)
            raise RuntimeError(f"Error listing offsets for topic {partition.topic} partition {partition.partition}: {e}")

    return {'status': 'success', 'message': results}


def delete_records(a: AdminClient, args: list[str]) -> dict[str, dict[str, str]]:
    """
    Delete records from the given topics and partitions.

    Args:
        a (AdminClient): The Kafka AdminClient instance.
        args (list[str]): List of arguments where each set of three represents a topic, partition, and offset.

    Returns:
        dict[str, dict[str, str]]: A dictionary with topic names as keys and a dictionary with partition information as values.
    """
    if len(args) == 0:
        raise ValueError(
            "Invalid number of arguments for delete_records, expected at least 3 " +
            "(Usage: delete_records <topic1> <partition1> <offset1> [<topic2> <partition2> <offset2> ..])")
    if len(args) % 3 != 0:
        raise ValueError(
            "Invalid number of arguments for delete_records " +
            "(Usage: delete_records <topic1> <partition1> <offset1> [<topic2> <partition2> <offset2> ..])")

    logging.info("Deleting records with args: %s", args)
    topic_partition_offsets = [
        TopicPartition(topic, int(partition), int(offset))
        for topic, partition, offset in zip(args[::3], args[1::3], args[2::3])
    ]

    futmap = a.delete_records(topic_partition_offsets)
    results = {}

    for partition, fut in futmap.items():
        try:
            result = fut.result()
            logging.info("Records deleted in topic %s partition %d before offset %d. The minimum offset in this partition is now %d",
                            partition.topic, partition.partition, partition.offset, result.low_watermark)
            results[partition.topic] = results.get(partition.topic, {})
            results[partition.topic][partition.partition] = {
                'offset': partition.offset,
                'low_watermark': result.low_watermark
            }
        except KafkaException as e:
            logging.error("Error deleting records in topic %s partition %d before offset %d: %s",
                            partition.topic, partition.partition, partition.offset, e)
            raise RuntimeError(f"Error deleting records in topic {partition.topic} partition {partition.partition} before offset {partition.offset}: {e}")

    return {'status': 'success', 'message': results}