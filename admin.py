
#!/usr/bin/env python
#
# Copyright 2018 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


# Example use of AdminClient operations.

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

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from typing import List, Optional

logging.basicConfig()

app = FastAPI()

class TopicModel(BaseModel):
    name: str

class PartitionModel(BaseModel):
    topic: str
    new_total_count: int

class ConfigModel(BaseModel):
    resource_type: str
    resource_name: str
    configs: str

class AclModel(BaseModel):
    resource_type: str
    resource_name: str
    resource_pattern_type: str
    principal: str
    host: str
    operation: str
    permission_type: str

class ConsumerGroupModel(BaseModel):
    group: str
    topic: Optional[str] = None
    partition: Optional[int] = None
    offset: Optional[int] = None

@app.post("/create_topics", tags=["topics"], description="Create new topics")
def create_topics(topics: List[TopicModel]):
    # Implement the logic to create topics
    pass

@app.post("/delete_topics", tags=["topics"], description="Delete topics")
def delete_topics(topics: List[TopicModel]):
    # Implement the logic to delete topics
    pass

@app.post("/create_partitions", tags=["partitions"], description="Create new partitions for topics")
def create_partitions(partitions: List[PartitionModel]):
    # Implement the logic to create partitions
    pass

@app.post("/describe_configs", tags=["configs"], description="Describe configurations of resources")
def describe_configs(configs: List[ConfigModel]):
    # Implement the logic to describe configs
    pass

@app.post("/alter_configs", tags=["configs"], description="Alter configurations of resources")
def alter_configs(configs: List[ConfigModel]):
    # Implement the logic to alter configs
    pass

@app.post("/create_acls", tags=["acls"], description="Create ACLs")
def create_acls(acls: List[AclModel]):
    # Implement the logic to create ACLs
    pass

@app.post("/delete_acls", tags=["acls"], description="Delete ACLs")
def delete_acls(acls: List[AclModel]):
    # Implement the logic to delete ACLs
    pass

@app.post("/list_consumer_group_offsets", tags=["consumer_groups"], description="List consumer group offsets")
def list_consumer_group_offsets(consumer_group: ConsumerGroupModel):
    # Implement the logic to list consumer group offsets
    pass

@app.post("/alter_consumer_group_offsets", tags=["consumer_groups"], description="Alter consumer group offsets")
def alter_consumer_group_offsets(consumer_group: ConsumerGroupModel):
    # Implement the logic to alter consumer group offsets
    pass

# Add more routes as needed following the same pattern


if __name__ == '__main__':
    if len(sys.argv) < 3:
        sys.stderr.write('Usage: %s <bootstrap-brokers> <operation> <args..>\n\n' % sys.argv[0])
        sys.stderr.write('operations:\n')
        sys.stderr.write(' create_topics <topic1> <topic2> ..\n')
        sys.stderr.write(' delete_topics <topic1> <topic2> ..\n')
        sys.stderr.write(' create_partitions <topic1> <new_total_count1> <topic2> <new_total_count2> ..\n')
        sys.stderr.write(' describe_configs <resource_type1> <resource_name1> <resource2> <resource_name2> ..\n')
        sys.stderr.write(' alter_configs <resource_type1> <resource_name1> ' +
                         '<config=val,config2=val2> <resource_type2> <resource_name2> <config..> ..\n')
        sys.stderr.write(' incremental_alter_configs <resource_type1> <resource_name1> ' +
                         '<config1=op1:val1;config2=op2:val2;config3=DELETE> ' +
                         '<resource_type2> <resource_name2> <config1=op1:..> ..\n')
        sys.stderr.write(' delta_alter_configs <resource_type1> <resource_name1> ' +
                         '<config=val,config2=val2> <resource_type2> <resource_name2> <config..> ..\n')
        sys.stderr.write(' create_acls <resource_type1> <resource_name1> <resource_patter_type1> ' +
                         '<principal1> <host1> <operation1> <permission_type1> ..\n')
        sys.stderr.write(' describe_acls <resource_type1 <resource_name1> <resource_patter_type1> ' +
                         '<principal1> <host1> <operation1> <permission_type1> ..\n')
        sys.stderr.write(' delete_acls <resource_type1> <resource_name1> <resource_patter_type1> ' +
                         '<principal1> <host1> <operation1> <permission_type1> ..\n')
        sys.stderr.write(' list [<all|topics|brokers|groups>]\n')
        sys.stderr.write(' list_consumer_groups [-states <state1>,<state2>,..] ' +
                         '[-types <type1>,<type2>,..]\n')
        sys.stderr.write(' describe_consumer_groups <include_authorized_operations> <group1> <group2> ..\n')
        sys.stderr.write(' describe_topics <include_authorized_operations> <topic1> <topic2> ..\n')
        sys.stderr.write(' describe_cluster <include_authorized_operations>\n')
        sys.stderr.write(' delete_consumer_groups <group1> <group2> ..\n')
        sys.stderr.write(' list_consumer_group_offsets <group> [<topic1> <partition1> <topic2> <partition2> ..]\n')
        sys.stderr.write(
            ' alter_consumer_group_offsets <group> <topic1> <partition1> <offset1> ' +
            '<topic2> <partition2> <offset2> ..\n')
        sys.stderr.write(' describe_user_scram_credentials [<user1> <user2> ..]\n')
        sys.stderr.write(' alter_user_scram_credentials UPSERT <user1> <mechanism1> ' +
                         '<iterations1> <password1> <salt1> ' +
                         '[UPSERT <user2> <mechanism2> <iterations2> ' +
                         ' <password2> <salt2> DELETE <user3> <mechanism3> ..]\n')
        sys.stderr.write(' list_offsets <isolation_level> <topic1> <partition1> <offset_spec1> ' +
                         '[<topic2> <partition2> <offset_spec2> ..]\n')
        sys.stderr.write(' delete_records <topic1> <partition1> <offset1> [<topic2> <partition2> <offset2> ..]\n')
        sys.stderr.write(' elect_leaders <election_type> [<topic1> <partition1> <topic2> <partition2> ..]\n')
        sys.exit(1)

    broker = sys.argv[1]
    operation = sys.argv[2]
    args = sys.argv[3:]

    # Create Admin client
    a = AdminClient({'bootstrap.servers': broker})

    opsmap = {'create_topics': create_topics,
              'delete_topics': delete_topics,
              'create_partitions': create_partitions,
              'describe_configs': describe_configs,
              'alter_configs': alter_configs,
              'incremental_alter_configs': incremental_alter_configs,
              'delta_alter_configs': delta_alter_configs,
              'create_acls': create_acls,
              'describe_acls': describe_acls,
              'delete_acls': delete_acls,
              'list': list,
              'list_consumer_groups': list_consumer_groups,
              'describe_consumer_groups': describe_consumer_groups,
              'describe_topics': describe_topics,
              'describe_cluster': describe_cluster,
              'delete_consumer_groups': delete_consumer_groups,
              'list_consumer_group_offsets': list_consumer_group_offsets,
              'alter_consumer_group_offsets': alter_consumer_group_offsets,
              'describe_user_scram_credentials': describe_user_scram_credentials,
              'alter_user_scram_credentials': alter_user_scram_credentials,
              'list_offsets': list_offsets,
              'delete_records': delete_records,
              'elect_leaders': elect_leaders}

    if operation not in opsmap:
        sys.stderr.write('Unknown operation: %s\n' % operation)
        sys.exit(1)

    opsmap[operation](a, args)
