# Kafka CLI Help

```
kafka - Universal Kafka Command-Line Interface

Usage: kafka [COMMAND] [OPTIONS]

A simplified, unified interface for Apache Kafka operations.

TOPIC COMMANDS:
  topics                Manage Kafka topics (create, list, describe, delete, alter)

PRODUCER/CONSUMER COMMANDS:
  produce               Send messages to a topic
  consume               Read messages from a topic
  share-consume         Read messages from a topic using share groups

GROUP COMMANDS:
  consumer-groups       Manage consumer groups (list, describe, delete, reset offsets)
  share-groups          Manage share groups (list, describe, delete, reset offsets)
  streams-groups        Manage Kafka Streams groups (list, describe)
  groups                List all groups (consumer, share, and streams)

CONFIGURATION COMMANDS:
  configs               View and modify broker/topic/client/user/ip configurations
  client-metrics        Manipulate and describe client metrics configurations

SECURITY COMMANDS:
  acls                  Manage Access Control Lists for security
  delegation-tokens     Create, renew, expire, or describe delegation tokens

CLUSTER COMMANDS:
  cluster               Display cluster-level information
  broker-api-versions   Retrieve broker version information
  log-dirs              Query log directory usage on brokers
  reassign-partitions   Reassign partitions across brokers
  leader-election       Trigger preferred replica leader election
  metadata-quorum       Describe the metadata quorum
  features              Manage feature flags
  storage               Manage storage and log directories

PERFORMANCE & TESTING COMMANDS:
  consumer-perf-test    Run consumer performance tests
  producer-perf-test    Run producer performance tests
  share-consumer-perf-test  Run share consumer performance tests
  e2e-latency           Measure end-to-end latency
  verifiable-producer   Run a verifiable producer for testing
  verifiable-consumer   Run a verifiable consumer for testing
  verifiable-share-consumer  Run a verifiable share consumer for testing

ADVANCED COMMANDS:
  transactions          Manage transactions
  replica-verification  Validate replica consistency
  delete-records        Delete records from partitions up to a specified offset
  get-offsets           Get topic partition offsets
  dump-log              Dump log file contents
  jmx                   Dump JMX metrics to standard output
  metadata-shell        Interactive metadata shell
  streams-resetter      Reset Kafka Streams application state

GLOBAL OPTIONS:
  --help, -h            Show this help message
  --version, -v         Show version information

EXAMPLES:
  kafka topics --list --bootstrap-server localhost:9092
  kafka produce my-topic --bootstrap-server localhost:9092
  kafka consume my-topic --from-beginning --bootstrap-server localhost:9092
  kafka consumer-groups --describe --group my-group --bootstrap-server localhost:9092
  kafka acls --list --bootstrap-server localhost:9092

For detailed help on any command, run:
  kafka [COMMAND] --help

Documentation: https://kafka.apache.org/documentation/
```
