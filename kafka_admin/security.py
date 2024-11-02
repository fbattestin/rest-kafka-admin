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

def create_acls(a: AdminClient, args: list[str]) -> dict[str, dict[str, str]]:
    """Create ACLs in the Kafka cluster.

    Args:
        a (AdminClient): The Kafka AdminClient instance.
        args (list[str]): List of ACL arguments.

    Returns:
        dict[str, dict[str, str]]: A dictionary with ACL descriptions as keys and a dictionary with 'status' and 'message' as values.
    """
    acl_bindings = [
        AclBinding(
            ResourceType[restype],
            parse_nullable_string(resname),
            ResourcePatternType[resource_pattern_type],
            parse_nullable_string(principal),
            parse_nullable_string(host),
            AclOperation[operation],
            AclPermissionType[permission_type]
        )
        for restype, resname, resource_pattern_type,
        principal, host, operation, permission_type
        in zip(
            args[0::7],
            args[1::7],
            args[2::7],
            args[3::7],
            args[4::7],
            args[5::7],
            args[6::7],
        )
    ]

    results = {}
    try:
        fs = a.create_acls(acl_bindings, request_timeout=10)
    except ValueError as e:
        logging.error(f"create_acls() failed: {e}")
        raise RuntimeError(f"create_acls() failed: {e}")

    for res, f in fs.items():
        try:
            f.result()
            logging.info(f"Created ACL {res}")
            results[res] = {'status': 'success', 'message': 'ACL created successfully'}
        except KafkaException as e:
            logging.error(f"Failed to create ACL {res}: {e}")
            results[res] = {'status': 'failure', 'message': str(e)}
        except Exception as e:
            logging.error(f"Unexpected error: {e}")
            raise

    return results


def describe_acls(a: AdminClient, args: list[str]) -> dict[str, dict[str, str]]:
    """Describe ACLs in the Kafka cluster.

    Args:
        a (AdminClient): The Kafka AdminClient instance.
        args (list[str]): List of ACL arguments.

    Returns:
        dict[str, dict[str, str]]: A dictionary with ACL descriptions as keys and a dictionary with 'status' and 'message' as values.
    """
    acl_binding_filters = [
        AclBindingFilter(
            ResourceType[restype],
            parse_nullable_string(resname),
            ResourcePatternType[resource_pattern_type],
            parse_nullable_string(principal),
            parse_nullable_string(host),
            AclOperation[operation],
            AclPermissionType[permission_type]
        )
        for restype, resname, resource_pattern_type,
        principal, host, operation, permission_type
        in zip(
            args[0::7],
            args[1::7],
            args[2::7],
            args[3::7],
            args[4::7],
            args[5::7],
            args[6::7],
        )
    ]

    results = {}
    fs = [
        a.describe_acls(acl_binding_filter, request_timeout=10)
        for acl_binding_filter in acl_binding_filters
    ]

    for acl_binding_filter, f in zip(acl_binding_filters, fs):
        try:
            acl_bindings = f.result()
            logging.info(f"Acls matching filter: {acl_binding_filter}")
            results[str(acl_binding_filter)] = {'status': 'success', 'message': [str(acl_binding) for acl_binding in acl_bindings]}
        except KafkaException as e:
            logging.error(f"Failed to describe {acl_binding_filter}: {e}")
            results[str(acl_binding_filter)] = {'status': 'failure', 'message': str(e)}
        except Exception as e:
            logging.error(f"Unexpected error: {e}")
            raise

    return results


def delete_acls(a: AdminClient, args: list[str]) -> dict[str, dict[str, str]]:
    """Delete ACLs in the Kafka cluster.

    Args:
        a (AdminClient): The Kafka AdminClient instance.
        args (list[str]): List of ACL arguments.

    Returns:
        dict[str, dict[str, str]]: A dictionary with ACL descriptions as keys and a dictionary with 'status' and 'message' as values.
    """
    acl_binding_filters = [
        AclBindingFilter(
            ResourceType[restype],
            parse_nullable_string(resname),
            ResourcePatternType[resource_pattern_type],
            parse_nullable_string(principal),
            parse_nullable_string(host),
            AclOperation[operation],
            AclPermissionType[permission_type]
        )
        for restype, resname, resource_pattern_type,
        principal, host, operation, permission_type
        in zip(
            args[0::7],
            args[1::7],
            args[2::7],
            args[3::7],
            args[4::7],
            args[5::7],
            args[6::7],
        )
    ]

    results = {}
    try:
        fs = a.delete_acls(acl_binding_filters, request_timeout=10)
    except ValueError as e:
        logging.error(f"delete_acls() failed: {e}")
        raise RuntimeError(f"delete_acls() failed: {e}")

    for res, f in fs.items():
        try:
            acl_bindings = f.result()
            logging.info(f"Deleted ACLs matching filter: {res}")
            results[res] = {'status': 'success', 'message': [str(acl_binding) for acl_binding in acl_bindings]}
        except KafkaException as e:
            logging.error(f"Failed to delete {res}: {e}")
            results[res] = {'status': 'failure', 'message': str(e)}
        except Exception as e:
            logging.error(f"Unexpected error: {e}")
            raise

    return results


def describe_user_scram_credentials(a: AdminClient, args: list[str]) -> dict[str, dict[str, str]]:
    """Describe User Scram Credentials.

    Args:
        a (AdminClient): The Kafka AdminClient instance.
        args (list[str]): List of usernames to describe.

    Returns:
        dict[str, dict[str, str]]: A dictionary with usernames as keys and a dictionary with 'status' and 'message' as values.
    """
    results = {}

    if len(args) == 0:
        # Case: Describes all user scram credentials
        f = a.describe_user_scram_credentials()
        try:
            user_scram_credentials = f.result()
            for username, response in user_scram_credentials.items():
                logging.info(f"Username: {username}")
                credentials_info = [
                    f"Mechanism: {info.mechanism}, Iterations: {info.iterations}"
                    for info in response.scram_credential_infos
                ]
                results[username] = {'status': 'success', 'message': credentials_info}
        except KafkaException as e:
            logging.error(f"Failed to describe all user scram credentials: {e}")
            raise RuntimeError(f"Failed to describe all user scram credentials: {e}")
        except Exception as e:
            logging.error(f"Unexpected error: {e}")
            raise
    else:
        # Case: Describe specified user scram credentials
        futmap = a.describe_user_scram_credentials(args)
        for username, fut in futmap.items():
            try:
                response = fut.result()
                logging.info(f"Username: {username}")
                credentials_info = [
                    f"Mechanism: {info.mechanism}, Iterations: {info.iterations}"
                    for info in response.scram_credential_infos
                ]
                results[username] = {'status': 'success', 'message': credentials_info}
            except KafkaException as e:
                logging.error(f"Failed to describe user scram credentials for {username}: {e}")
                results[username] = {'status': 'failure', 'message': str(e)}
            except Exception as e:
                logging.error(f"Unexpected error: {e}")
                raise

    return results


def alter_user_scram_credentials(a: AdminClient, args: list[str]) -> dict[str, dict[str, str]]:
    """Alter User Scram Credentials.

    Args:
        a (AdminClient): The Kafka AdminClient instance.
        args (list[str]): List of alteration arguments.

    Returns:
        dict[str, dict[str, str]]: A dictionary with usernames as keys and a dictionary with 'status' and 'message' as values.
    """
    alterations = []
    results = {}
    i = 0

    while i < len(args):
        op = args[i]
        if op == "UPSERT":
            if i + 5 >= len(args):
                raise ValueError(f"Invalid number of arguments for alteration, expected 5, got {len(args) - i - 1}")
            user = args[i + 1]
            mechanism = ScramMechanism[args[i + 2]]
            iterations = int(args[i + 3])
            password = bytes(args[i + 4], 'utf8')
            salt = bytes(args[i + 5], 'utf8') if args[i + 5] else None
            scram_credential_info = ScramCredentialInfo(mechanism, iterations)
            upsertion = UserScramCredentialUpsertion(user, scram_credential_info, password, salt)
            alterations.append(upsertion)
            i += 6
        elif op == "DELETE":
            if i + 2 >= len(args):
                raise ValueError(f"Invalid number of arguments for alteration, expected 2, got {len(args) - i - 1}")
            user = args[i + 1]
            mechanism = ScramMechanism[args[i + 2]]
            deletion = UserScramCredentialDeletion(user, mechanism)
            alterations.append(deletion)
            i += 3
        else:
            raise ValueError(f"Invalid alteration {op}, must be UPSERT or DELETE")

    futmap = a.alter_user_scram_credentials(alterations)
    for username, fut in futmap.items():
        try:
            fut.result()
            logging.info(f"{username}: Success")
            results[username] = {'status': 'success', 'message': 'Alteration successful'}
        except KafkaException as e:
            logging.error(f"{username}: Error: {e}")
            results[username] = {'status': 'failure', 'message': str(e)}
        except Exception as e:
            logging.error(f"Unexpected error: {e}")
            raise

    return results
