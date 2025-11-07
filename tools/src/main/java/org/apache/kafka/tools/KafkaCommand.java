/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.tools;

import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.tools.consumer.group.ConsumerGroupCommand;
import org.apache.kafka.tools.consumer.group.ShareGroupCommand;
import org.apache.kafka.tools.reassign.ReassignPartitionsCommand;
import org.apache.kafka.tools.streams.StreamsGroupCommand;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Unified Kafka Command-Line Interface
 * Provides a single entry point for all Kafka CLI tools with organized help messages
 */
public class KafkaCommand {

    private static final Map<String, CommandInfo> COMMANDS = new LinkedHashMap<>();

    static class CommandInfo {
        final String name;
        final String description;
        final Class<?> commandClass;
        final String methodName;
        final List<String> aliases;

        CommandInfo(String name, String description, Class<?> commandClass, String methodName, String... aliases) {
            this.name = name;
            this.description = description;
            this.commandClass = commandClass;
            this.methodName = methodName;
            this.aliases = Arrays.asList(aliases);
        }
    }

    static {
        // Topic Commands
        COMMANDS.put("topics", new CommandInfo(
            "topics",
            "Manage Kafka topics (create, list, describe, delete, alter)",
            TopicCommand.class,
            "main",
            "topic"
        ));

        // Producer/Consumer Commands
        COMMANDS.put("produce", new CommandInfo(
            "produce",
            "Send messages to a topic (read from standard input)",
            ConsoleProducer.class,
            "main",
            "producer", "console-producer"
        ));

        COMMANDS.put("consume", new CommandInfo(
            "consume",
            "Read messages from a topic and output to standard output",
            org.apache.kafka.tools.consumer.ConsoleConsumer.class,
            "main",
            "consumer", "console-consumer"
        ));

        COMMANDS.put("share-consume", new CommandInfo(
            "share-consume",
            "Read messages from a topic using share groups",
            org.apache.kafka.tools.consumer.ConsoleShareConsumer.class,
            "main",
            "share-consumer", "console-share-consumer"
        ));

        // Group Commands
        COMMANDS.put("consumer-groups", new CommandInfo(
            "consumer-groups",
            "Manage consumer groups (list, describe, delete, reset offsets)",
            ConsumerGroupCommand.class,
            "main",
            "consumer-group"
        ));

        COMMANDS.put("share-groups", new CommandInfo(
            "share-groups",
            "Manage share groups (list, describe, delete, reset offsets)",
            ShareGroupCommand.class,
            "main",
            "share-group"
        ));

        COMMANDS.put("streams-groups", new CommandInfo(
            "streams-groups",
            "Manage Kafka Streams groups (list, describe)",
            StreamsGroupCommand.class,
            "main",
            "streams-group"
        ));

        COMMANDS.put("groups", new CommandInfo(
            "groups",
            "List all groups (consumer, share, and streams)",
            GroupsCommand.class,
            "main",
            "group"
        ));

        // Configuration Commands
        try {
            COMMANDS.put("configs", new CommandInfo(
                "configs",
                "View and modify broker/topic/client/user/ip configurations",
                Class.forName("kafka.admin.ConfigCommand"),
                "main",
                "config"
            ));
        } catch (ClassNotFoundException e) {
            // ConfigCommand might not be available in all build configurations
        }

        COMMANDS.put("client-metrics", new CommandInfo(
            "client-metrics",
            "Manipulate and describe client metrics configurations",
            ClientMetricsCommand.class,
            "main"
        ));

        // Security Commands
        COMMANDS.put("acls", new CommandInfo(
            "acls",
            "Manage Access Control Lists for security",
            AclCommand.class,
            "main",
            "acl"
        ));

        COMMANDS.put("delegation-tokens", new CommandInfo(
            "delegation-tokens",
            "Create, renew, expire, or describe delegation tokens",
            DelegationTokenCommand.class,
            "main",
            "delegation-token", "tokens", "token"
        ));

        // Cluster Commands
        COMMANDS.put("cluster", new CommandInfo(
            "cluster",
            "Display cluster-level information",
            ClusterTool.class,
            "main"
        ));

        COMMANDS.put("broker-api-versions", new CommandInfo(
            "broker-api-versions",
            "Retrieve broker version information",
            BrokerApiVersionsCommand.class,
            "main",
            "api-versions"
        ));

        COMMANDS.put("log-dirs", new CommandInfo(
            "log-dirs",
            "Query log directory usage on brokers",
            LogDirsCommand.class,
            "main",
            "logdirs"
        ));

        COMMANDS.put("reassign-partitions", new CommandInfo(
            "reassign-partitions",
            "Reassign partitions across brokers",
            ReassignPartitionsCommand.class,
            "main",
            "reassign"
        ));

        COMMANDS.put("leader-election", new CommandInfo(
            "leader-election",
            "Trigger preferred replica leader election",
            LeaderElectionCommand.class,
            "main"
        ));

        COMMANDS.put("metadata-quorum", new CommandInfo(
            "metadata-quorum",
            "Describe the metadata quorum",
            MetadataQuorumCommand.class,
            "main",
            "quorum"
        ));

        COMMANDS.put("features", new CommandInfo(
            "features",
            "Manage feature flags",
            FeatureCommand.class,
            "main",
            "feature"
        ));

        try {
            COMMANDS.put("storage", new CommandInfo(
                "storage",
                "Manage storage and log directories",
                Class.forName("kafka.tools.StorageTool"),
                "main"
            ));
        } catch (ClassNotFoundException e) {
            // StorageTool might not be available in all build configurations
        }

        // Performance & Testing Commands
        COMMANDS.put("consumer-perf-test", new CommandInfo(
            "consumer-perf-test",
            "Run consumer performance tests",
            ConsumerPerformance.class,
            "main",
            "consumer-perf"
        ));

        COMMANDS.put("producer-perf-test", new CommandInfo(
            "producer-perf-test",
            "Run producer performance tests",
            ProducerPerformance.class,
            "main",
            "producer-perf"
        ));

        COMMANDS.put("share-consumer-perf-test", new CommandInfo(
            "share-consumer-perf-test",
            "Run share consumer performance tests",
            ShareConsumerPerformance.class,
            "main",
            "share-consumer-perf"
        ));

        COMMANDS.put("e2e-latency", new CommandInfo(
            "e2e-latency",
            "Measure end-to-end latency",
            EndToEndLatency.class,
            "main",
            "latency"
        ));

        COMMANDS.put("verifiable-producer", new CommandInfo(
            "verifiable-producer",
            "Run a verifiable producer for testing",
            VerifiableProducer.class,
            "main"
        ));

        COMMANDS.put("verifiable-consumer", new CommandInfo(
            "verifiable-consumer",
            "Run a verifiable consumer for testing",
            VerifiableConsumer.class,
            "main"
        ));

        COMMANDS.put("verifiable-share-consumer", new CommandInfo(
            "verifiable-share-consumer",
            "Run a verifiable share consumer for testing",
            VerifiableShareConsumer.class,
            "main"
        ));

        // Advanced Commands
        COMMANDS.put("transactions", new CommandInfo(
            "transactions",
            "Manage transactions",
            TransactionsCommand.class,
            "main",
            "transaction"
        ));

        COMMANDS.put("replica-verification", new CommandInfo(
            "replica-verification",
            "Validate replica consistency",
            ReplicaVerificationTool.class,
            "main",
            "verify-replicas"
        ));

        COMMANDS.put("delete-records", new CommandInfo(
            "delete-records",
            "Delete records from partitions up to a specified offset",
            DeleteRecordsCommand.class,
            "main"
        ));

        COMMANDS.put("get-offsets", new CommandInfo(
            "get-offsets",
            "Get topic partition offsets",
            GetOffsetShell.class,
            "main",
            "offsets"
        ));

        COMMANDS.put("dump-log", new CommandInfo(
            "dump-log",
            "Dump log file contents",
            DumpLogSegments.class,
            "main"
        ));

        COMMANDS.put("jmx", new CommandInfo(
            "jmx",
            "Dump JMX metrics to standard output",
            JmxTool.class,
            "main"
        ));

        COMMANDS.put("metadata-shell", new CommandInfo(
            "metadata-shell",
            "Interactive metadata shell",
            org.apache.kafka.shell.MetadataShell.class,
            "main",
            "shell"
        ));

        COMMANDS.put("streams-resetter", new CommandInfo(
            "streams-resetter",
            "Reset Kafka Streams application state",
            StreamsResetter.class,
            "main",
            "streams-reset"
        ));
    }

    public static void main(String[] args) {
        Exit.exit(mainNoExit(args));
    }

    static int mainNoExit(String[] args) {
        if (args.length == 0 || args[0].equals("--help") || args[0].equals("-h")) {
            printHelp();
            return 0;
        }

        if (args[0].equals("--version") || args[0].equals("-v")) {
            printVersion();
            return 0;
        }

        String command = args[0];
        CommandInfo commandInfo = findCommand(command);

        if (commandInfo == null) {
            System.err.println("Error: Unknown command '" + command + "'");
            System.err.println();
            System.err.println("Run 'kafka --help' to see available commands");
            return 1;
        }

        // Remove the command from args and pass the rest to the actual command
        String[] commandArgs = Arrays.copyOfRange(args, 1, args.length);

        try {
            // Invoke the command's main method
            java.lang.reflect.Method method = commandInfo.commandClass.getMethod(commandInfo.methodName, String[].class);
            method.invoke(null, (Object) commandArgs);
            return 0;
        } catch (java.lang.reflect.InvocationTargetException e) {
            Throwable cause = e.getCause();
            if (cause != null) {
                System.err.println(cause.getMessage());
                if (cause instanceof Exception) {
                    cause.printStackTrace(System.err);
                }
            } else {
                System.err.println(e.getMessage());
                e.printStackTrace(System.err);
            }
            return 1;
        } catch (Exception e) {
            System.err.println("Error executing command: " + e.getMessage());
            e.printStackTrace(System.err);
            return 1;
        }
    }

    private static CommandInfo findCommand(String command) {
        // First try direct lookup
        CommandInfo info = COMMANDS.get(command);
        if (info != null) {
            return info;
        }

        // Then try aliases
        for (CommandInfo cmdInfo : COMMANDS.values()) {
            if (cmdInfo.aliases.contains(command)) {
                return cmdInfo;
            }
        }

        return null;
    }

    private static void printHelp() {
        System.out.println("kafka - Universal Kafka Command-Line Interface");
        System.out.println();
        System.out.println("Usage: kafka [COMMAND] [OPTIONS]");
        System.out.println();
        System.out.println("A simplified, unified interface for Apache Kafka operations.");
        System.out.println();

        System.out.println("TOPIC COMMANDS:");
        printCommandsByPrefix("topics");
        System.out.println();

        System.out.println("PRODUCER/CONSUMER COMMANDS:");
        printCommandsByPrefix("produce", "consume", "share-consume");
        System.out.println();

        System.out.println("GROUP COMMANDS:");
        printCommandsByPrefix("consumer-groups", "share-groups", "streams-groups", "groups");
        System.out.println();

        System.out.println("CONFIGURATION COMMANDS:");
        printCommandsByPrefix("configs", "client-metrics");
        System.out.println();

        System.out.println("SECURITY COMMANDS:");
        printCommandsByPrefix("acls", "delegation-tokens");
        System.out.println();

        System.out.println("CLUSTER COMMANDS:");
        printCommandsByPrefix("cluster", "broker-api-versions", "log-dirs", "reassign-partitions",
                            "leader-election", "metadata-quorum", "features", "storage");
        System.out.println();

        System.out.println("PERFORMANCE & TESTING COMMANDS:");
        printCommandsByPrefix("consumer-perf-test", "producer-perf-test", "share-consumer-perf-test",
                            "e2e-latency", "verifiable-producer", "verifiable-consumer", "verifiable-share-consumer");
        System.out.println();

        System.out.println("ADVANCED COMMANDS:");
        printCommandsByPrefix("transactions", "replica-verification", "delete-records",
                            "get-offsets", "dump-log", "jmx", "metadata-shell", "streams-resetter");
        System.out.println();

        System.out.println("GLOBAL OPTIONS:");
        System.out.println("  --help, -h            Show this help message");
        System.out.println("  --version, -v         Show version information");
        System.out.println();

        System.out.println("EXAMPLES:");
        System.out.println("  kafka topics --list --bootstrap-server localhost:9092");
        System.out.println("  kafka produce my-topic --bootstrap-server localhost:9092");
        System.out.println("  kafka consume my-topic --from-beginning --bootstrap-server localhost:9092");
        System.out.println("  kafka consumer-groups --describe --group my-group --bootstrap-server localhost:9092");
        System.out.println("  kafka acls --list --bootstrap-server localhost:9092");
        System.out.println();

        System.out.println("For detailed help on any command, run:");
        System.out.println("  kafka [COMMAND] --help");
        System.out.println();
        System.out.println("Documentation: https://kafka.apache.org/documentation/");
    }

    private static void printCommandsByPrefix(String... commandNames) {
        for (String name : commandNames) {
            CommandInfo info = COMMANDS.get(name);
            if (info != null) {
                System.out.printf("  %-24s %s%n", info.name, info.description);
            }
        }
    }

    private static void printVersion() {
        System.out.println("Apache Kafka CLI Tools");
        System.out.println("Run any command with --version to see specific version information");
    }
}
