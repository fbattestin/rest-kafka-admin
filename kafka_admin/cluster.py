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


def describe(a: AdminClient, args: list[str]) -> dict[str, dict[str, str]]:
    """Describe the Kafka cluster.

    Args:
        a (AdminClient): The Kafka AdminClient instance.
        args (list[str]): List of arguments, where the first argument is a flag to include authorized operations.

    Returns:
        dict[str, dict[str, str]]: A dictionary with cluster details.
    """
    include_auth_ops = bool(int(args[0]))
    logging.info(f"Describing cluster with include_auth_ops={include_auth_ops}")
    future = a.describe_cluster(request_timeout=10, include_authorized_operations=include_auth_ops)
    result = {}

    try:
        c = future.result()
        result['cluster_id'] = c.cluster_id
        result['controller'] = str(c.controller) if c.controller else "No Controller Information Available"
        result['nodes'] = [str(node) for node in c.nodes]

        if include_auth_ops:
            result['authorized_operations'] = [acl_op.name for acl_op in c.authorized_operations]

        logging.info("Cluster described successfully")
    except KafkaException as e:
        logging.error(f"Error while describing cluster: {e}")
        raise RuntimeError(f"Error while describing cluster: {e}")
    except Exception as e:
        logging.error(f"Unexpected error while describing cluster: {e}")
        raise RuntimeError(f"Unexpected error while describing cluster: {e}")

    return {'status': 'success', 'message': result}


def print_config(config, depth):
    return {
        'name': (' ' * depth) + config.name,
        'value': config.value,
        'source': ConfigSource(config.source),
        'is_read_only': config.is_read_only,
        'is_default': config.is_default,
        'is_sensitive': config.is_sensitive,
        'is_synonym': config.is_synonym,
        'synonyms': [{"name": x.name, "source": ConfigSource(x.source)} for x in iter(config.synonyms.values())]
    }


def describe_configs(a: AdminClient, resources: list[tuple[ResourceType, str]]) -> dict[str, dict[str, str]]:
    """Describe configs for the given resources in the Kafka cluster.

    Args:
        a (AdminClient): The Kafka AdminClient instance.
        resources (list[tuple[ResourceType, str]]): List of tuples containing resource type and resource name.

    Returns:
        dict[str, dict[str, str]]: A dictionary with resource names as keys and a dictionary with 'status' and 'message' as values.
    """
    logging.info(f"Describing configs for resources: {resources}")
    config_resources = [ConfigResource(restype, resname) for restype, resname in resources]
    fs = a.describe_configs(config_resources)
    results = {}

    for res, f in fs.items():
        try:
            configs = f.result()
            config_details = [print_config(config, 1) for config in iter(configs.values())]
            results[res.name] = {'status': 'success', 'message': config_details}
            logging.info(f"Configs for resource {res} described")
        except KafkaException as e:
            logging.error(f"Failed to describe configs for resource {res}: {e}")
            raise RuntimeError(f"Failed to describe configs for resource {res.name}: {e}")
        except Exception as e:
            logging.error(f"Unexpected error while describing configs for resource {res}: {e}")
            raise RuntimeError(f"Unexpected error while describing configs for resource {res.name}: {e}")

    return results


def incremental_alter_configs(a: AdminClient, args: list[str]) -> dict[str, dict[str, str]]:
    """Incrementally alter configs, keeping non-specified configuration properties with their previous values.

    Args:
        a (AdminClient): The Kafka AdminClient instance.
        args (list[str]): List of arguments specifying the resources and their configurations.

    Returns:
        dict[str, dict[str, str]]: A dictionary with resource names as keys and a dictionary with 'status' and 'message' as values.
    """
    logging.info(f"Incrementally altering configs with args: {args}")
    resources = []
    for restype, resname, configs in zip(args[0::3], args[1::3], args[2::3]):
        incremental_configs = []
        for name, operation_and_value in [conf.split('=') for conf in configs.split(';')]:
            if operation_and_value == "DELETE":
                operation, value = operation_and_value, None
            else:
                operation, value = operation_and_value.split(':')
            operation = AlterConfigOpType[operation]
            incremental_configs.append(ConfigEntry(name, value, incremental_operation=operation))
        resources.append(ConfigResource(restype, resname, incremental_configs=incremental_configs))

    fs = a.incremental_alter_configs(resources)
    results = {}

    for res, f in fs.items():
        try:
            f.result()
            results[res.name] = {'status': 'success', 'message': f"{res} configuration successfully altered"}
            logging.info(f"{res} configuration successfully altered")
        except Exception as e:
            logging.error(f"Failed to alter configs for resource {res}: {e}")
            raise RuntimeError(f"Failed to alter configs for resource {res.name}: {e}")

    return results


def alter_configs(a: AdminClient, args: list[str]) -> dict[str, dict[str, str]]:
    """Alter configs atomically, replacing non-specified configuration properties with their default values.

    Args:
        a (AdminClient): The Kafka AdminClient instance.
        args (list[str]): List of arguments specifying the resources and their configurations.

    Returns:
        dict[str, dict[str, str]]: A dictionary with resource names as keys and a dictionary with 'status' and 'message' as values.
    """
    logging.info(f"Altering configs with args: {args}")
    resources = []
    for restype, resname, configs in zip(args[0::3], args[1::3], args[2::3]):
        resource = ConfigResource(restype, resname)
        resources.append(resource)
        for k, v in [conf.split('=') for conf in configs.split(',')]:
            resource.set_config(k, v)

    fs = a.alter_configs(resources)
    results = {}

    for res, f in fs.items():
        try:
            f.result()
            results[res.name] = {'status': 'success', 'message': f"{res} configuration successfully altered"}
            logging.info(f"{res} configuration successfully altered")
        except Exception as e:
            logging.error(f"Failed to alter configs for resource {res}: {e}")
            raise RuntimeError(f"Failed to alter configs for resource {res.name}: {e}")

    return results


def delta_alter_configs(a: AdminClient, args: list[str]) -> dict[str, dict[str, str]]:
    """Modify the supplied configuration entries by first reading the configuration from the broker,
    updating the supplied configuration with the broker configuration (without overwriting), and then writing it all back.

    Args:
        a (AdminClient): The Kafka AdminClient instance.
        args (list[str]): List of arguments specifying the resources and their configurations.

    Returns:
        dict[str, dict[str, str]]: A dictionary with resource names as keys and a dictionary with 'status' and 'message' as values.
    """
    logging.info(f"Delta altering configs with args: {args}")
    resources = []
    for restype, resname, configs in zip(args[0::3], args[1::3], args[2::3]):
        resource = ConfigResource(restype, resname)
        resources.append(resource)
        for k, v in [conf.split('=') for conf in configs.split(',')]:
            resource.set_config(k, v)

    class WaitZero:
        def __init__(self, waitcnt):
            self.cnt = waitcnt
            self.lock = threading.Lock()
            self.event = threading.Event()

        def decr(self):
            with self.lock:
                assert self.cnt > 0
                self.cnt -= 1
            self.event.set()

        def wait(self):
            with self.lock:
                while self.cnt > 0:
                    self.lock.release()
                    self.event.wait()
                    self.event.clear()
                    self.lock.acquire()
                self.lock.release()

        def __len__(self):
            with self.lock:
                return self.cnt

    wait_zero = WaitZero(len(resources))
    fs = a.describe_configs(resources)
    results = {}

    def delta_alter_configs_done(fut, resource):
        e = fut.exception()
        if e is not None:
            logging.error(f"Config update for {resource} failed: {e}")
        else:
            logging.info(f"Config for {resource} updated")
        wait_zero.decr()

    def delta_alter_configs(resource, remote_config):
        logging.info(f"Updating {len(resource)} supplied config entries {resource} with {len(remote_config)} config entries read from cluster")
        for k, entry in [(k, v) for k, v in remote_config.items() if not v.is_default]:
            resource.set_config(k, entry.value, overwrite=False)

        fs = a.alter_configs([resource])
        fs[resource].add_done_callback(lambda fut: delta_alter_configs_done(fut, resource))

    for res, f in fs.items():
        f.add_done_callback(lambda fut, resource=res: delta_alter_configs(resource, fut.result()))

    logging.info(f"Waiting for {len(wait_zero)} resource updates to finish")
    wait_zero.wait()

    for resource in resources:
        results[resource.name] = {'status': 'success', 'message': f"{resource} configuration successfully altered"}

    return results

def elect_leaders(a: AdminClient, args: list[str]) -> dict[str, dict[str, str]]:
    """Trigger leader election for the specified partitions.

    Args:
        a (AdminClient): The Kafka AdminClient instance.
        args (list[str]): List of arguments specifying the election type and partitions.

    Returns:
        dict[str, dict[str, str]]: A dictionary with partition details and their election status.
    """
    logging.info(f"Electing leaders with args: {args}")
    partitions = []
    if (len(args) - 1) % 2 != 0:
        raise ValueError("Invalid number of arguments for elect_leaders, Expected format: "
                            "elect_leaders <election_type> [<topic1> <partition1> <topic2> <partition2> ..]")

    try:
        election_type = ElectionType[args[0]]
    except KeyError:
        raise ValueError(f"Invalid election_type: {args[0]}, expected 'PREFERRED' or 'UNCLEAN'")

    for topic, partition in zip(args[1::2], args[2::2]):
        partitions.append(TopicPartition(topic, int(partition)))

    if len(partitions) == 0:
        partitions = None

    f = a.elect_leaders(election_type, partitions)
    results = {}

    try:
        election_results = f.result()
        logging.info(f"Elect leaders call returned {len(election_results)} result(s)")
        for partition, error in election_results.items():
            if error is None:
                results[f"{partition.topic}-{partition.partition}"] = {'status': 'success', 'message': f"Leader Election Successful for topic: '{partition.topic}' partition: '{partition.partition}'"}
                logging.info(f"Leader Election Successful for topic: '{partition.topic}' partition: '{partition.partition}'")
            else:
                results[f"{partition.topic}-{partition.partition}"] = {'status': 'failure', 'message': f"Leader Election Failed for topic: '{partition.topic}' partition: '{partition.partition}' error code: {error.code()} error message: {error.str()}"}
                logging.error(f"Leader Election Failed for topic: '{partition.topic}' partition: '{partition.partition}' error code: {error.code()} error message: {error.str()}")
    except KafkaException as e:
        logging.error(f"Error electing leaders: {e}")
        raise RuntimeError(f"Error electing leaders: {e}")

    return results