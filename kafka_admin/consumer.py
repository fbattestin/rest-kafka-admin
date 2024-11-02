from confluent_kafka import (KafkaException, ConsumerGroupTopicPartitions,
                             TopicPartition, ConsumerGroupState,
                             TopicCollection, IsolationLevel,
                             ConsumerGroupType, ElectionType)
from confluent_kafka.admin import (AdminClient, NewTopic, NewPartitions, ConfigResource,
                                   ConfigEntry, ConfigSource, AclBinding,
                                   AclBindingFilter, ResourceType, ResourcePatternType,
                                   AclOperation, AclPermissionType, AlterConfigOpType,
                                   ScramMechanism, ScramCredentialInfo,
                                   UserScramCredentialUpsertion, UserScramCredentialDeletion,
                                   OffsetSpec)
import sys
import threading
import logging
import argparse

logging.basicConfig()



def parse_list_consumer_groups_args(args, states, types):
    parser = argparse.ArgumentParser(prog='list_consumer_groups')
    parser.add_argument('-states')
    parser.add_argument('-types')
    parsed_args = parser.parse_args(args)

    def usage(message):
        print(message)
        parser.print_usage()
        sys.exit(1)

    if parsed_args.states:
        for arg in parsed_args.states.split(","):
            try:
                states.add(ConsumerGroupState[arg])
            except KeyError:
                usage(f"Invalid state: {arg}")
    if parsed_args.types:
        for arg in parsed_args.types.split(","):
            try:
                types.add(ConsumerGroupType[arg])
            except KeyError:
                usage(f"Invalid type: {arg}")


def list(a: AdminClient, args: list[str]) -> dict[str, dict[str, str]]:
    """List Consumer Groups in the Kafka cluster.

    Args:
        a (AdminClient): The Kafka AdminClient instance.
        args (list[str]): List of arguments for filtering consumer groups.

    Returns:
        dict[str, dict[str, str]]: A dictionary with consumer group IDs as keys and a dictionary with 'status' and 'message' as values.
    """
    states = set()
    types = set()
    parse_list_consumer_groups_args(args, states, types)

    future = a.list_consumer_groups(request_timeout=10, states=states, types=types)
    results = {}

    try:
        list_consumer_groups_result = future.result()
        logging.info(f"{len(list_consumer_groups_result.valid)} consumer groups found")
        for valid in list_consumer_groups_result.valid:
            logging.info(f"Consumer Group ID: {valid.group_id}, State: {valid.state}, Type: {valid.type}")
            results[valid.group_id] = {'status': 'success', 'message': 'Consumer group listed successfully'}
        for error in list_consumer_groups_result.errors:
            logging.error(f"Error listing consumer group: {error}")
            results[error.group_id] = {'status': 'error', 'message': str(error)}
    except Exception as e:
        logging.error(f"Failed to list consumer groups: {e}")
        raise RuntimeError(f"Failed to list consumer groups: {e}")

    return results


def describe(a: AdminClient, args: list[str]) -> dict[str, dict[str, str]]:
    """Describe Consumer Groups in the Kafka cluster.

    Args:
        a (AdminClient): The Kafka AdminClient instance.
        args (list[str]): List of consumer group IDs to describe.

    Returns:
        dict[str, dict[str, str]]: A dictionary with consumer group IDs as keys and a dictionary with 'status' and 'message' as values.
    """
    include_auth_ops = bool(int(args[0]))
    args = args[1:]
    futureMap = a.describe_consumer_groups(args, include_authorized_operations=include_auth_ops, request_timeout=10)
    results = {}

    for group_id, future in futureMap.items():
        try:
            g = future.result()
            group_info = {
                "is_simple_consumer_group": g.is_simple_consumer_group,
                "state": g.state,
                "partition_assignor": g.partition_assignor,
                "coordinator": str(g.coordinator),
                "members": []
            }
            for member in g.members:
                member_info = {
                    "member_id": member.member_id,
                    "host": member.host,
                    "client_id": member.client_id,
                    "group_instance_id": member.group_instance_id,
                    "assignments": [{"topic": toppar.topic, "partition": toppar.partition} for toppar in member.assignment.topic_partitions] if member.assignment else []
                }
                group_info["members"].append(member_info)
            if include_auth_ops:
                group_info["authorized_operations"] = [acl_op.name for acl_op in g.authorized_operations]
            results[group_id] = {'status': 'success', 'message': group_info}
        except KafkaException as e:
            logging.error(f"Error while describing group id '{group_id}': {e}")
            results[group_id] = {'status': 'error', 'message': str(e)}
        except Exception as e:
            logging.error(f"Failed to describe consumer group {group_id}: {e}")
            raise RuntimeError(f"Failed to describe consumer group {group_id}: {e}")

    return results


def delete_consumer_groups(a: AdminClient, args: list[str]) -> dict[str, dict[str, str]]:
    """Delete Consumer Groups in the Kafka cluster.

    Args:
        a (AdminClient): The Kafka AdminClient instance.
        args (list[str]): List of consumer group IDs to delete.

    Returns:
        dict[str, dict[str, str]]: A dictionary with consumer group IDs as keys and a dictionary with 'status' and 'message' as values.
    """
    futureMap = a.delete_consumer_groups(args, request_timeout=10)
    results = {}

    for group_id, future in futureMap.items():
        try:
            future.result()  # The result itself is None
            logging.info(f"Deleted group with id '{group_id}' successfully")
            results[group_id] = {'status': 'success', 'message': 'Consumer group deleted successfully'}
        except KafkaException as e:
            logging.error(f"Error deleting group id '{group_id}': {e}")
            results[group_id] = {'status': 'error', 'message': str(e)}
        except Exception as e:
            logging.error(f"Failed to delete consumer group {group_id}: {e}")
            raise RuntimeError(f"Failed to delete consumer group {group_id}: {e}")

    return results


def list_consumer_group_offsets(a: AdminClient, args: list[str]) -> dict[str, dict[str, str]]:
    """List consumer group offsets in the Kafka cluster.

    Args:
        a (AdminClient): The Kafka AdminClient instance.
        args (list[str]): List of arguments for listing consumer group offsets.

    Returns:
        dict[str, dict[str, str]]: A dictionary with consumer group IDs as keys and a dictionary with 'status' and 'message' as values.
    """
    topic_partitions = []
    for topic, partition in zip(args[1::2], args[2::2]):
        topic_partitions.append(TopicPartition(topic, int(partition)))
    if not topic_partitions:
        topic_partitions = None
    groups = [ConsumerGroupTopicPartitions(args[0], topic_partitions)]

    futureMap = a.list_consumer_group_offsets(groups)
    results = {}

    for group_id, future in futureMap.items():
        try:
            response_offset_info = future.result()
            logging.info(f"Group: {response_offset_info.group_id}")
            for topic_partition in response_offset_info.topic_partitions:
                if topic_partition.error:
                    logging.error(f"Error: {topic_partition.error.str()} occurred with {topic_partition.topic} [{topic_partition.partition}]")
                    results[group_id] = {'status': 'error', 'message': topic_partition.error.str()}
                else:
                    logging.info(f"{topic_partition.topic} [{topic_partition.partition}]: {topic_partition.offset}")
                    results[group_id] = {'status': 'success', 'message': 'Consumer group offsets listed successfully'}
        except KafkaException as e:
            logging.error(f"Failed to list {group_id}: {e}")
            results[group_id] = {'status': 'error', 'message': str(e)}
        except Exception as e:
            logging.error(f"Failed to list consumer group offsets for {group_id}: {e}")
            raise RuntimeError(f"Failed to list consumer group offsets for {group_id}: {e}")

    return results


def alter_consumer_group_offsets(a: AdminClient, args: list[str]) -> dict[str, dict[str, str]]:
    """Alter consumer group offsets in the Kafka cluster.

    Args:
        a (AdminClient): The Kafka AdminClient instance.
        args (list[str]): List of arguments for altering consumer group offsets.

    Returns:
        dict[str, dict[str, str]]: A dictionary with consumer group IDs as keys and a dictionary with 'status' and 'message' as values.
    """
    topic_partitions = []
    for topic, partition, offset in zip(args[1::3], args[2::3], args[3::3]):
        topic_partitions.append(TopicPartition(topic, int(partition), int(offset)))
    if not topic_partitions:
        topic_partitions = None
    groups = [ConsumerGroupTopicPartitions(args[0], topic_partitions)]

    futureMap = a.alter_consumer_group_offsets(groups)
    results = {}

    for group_id, future in futureMap.items():
        try:
            response_offset_info = future.result()
            logging.info(f"Group: {response_offset_info.group_id}")
            for topic_partition in response_offset_info.topic_partitions:
                if topic_partition.error:
                    logging.error(f"Error: {topic_partition.error.str()} occurred with {topic_partition.topic} [{topic_partition.partition}]")
                    results[group_id] = {'status': 'error', 'message': topic_partition.error.str()}
                else:
                    logging.info(f"{topic_partition.topic} [{topic_partition.partition}]: {topic_partition.offset}")
                    results[group_id] = {'status': 'success', 'message': 'Consumer group offsets altered successfully'}
        except KafkaException as e:
            logging.error(f"Failed to alter {group_id}: {e}")
            results[group_id] = {'status': 'error', 'message': str(e)}
        except Exception as e:
            logging.error(f"Failed to alter consumer group offsets for {group_id}: {e}")
            raise RuntimeError(f"Failed to alter consumer group offsets for {group_id}: {e}")

    return results
