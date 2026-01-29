package org.apache.kafka.demo;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.DeserializationExceptionHandler;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.processor.ProcessorContext;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;

/**
 * Demo: Error handling BEFORE KIP-1034 DLQ
 *
 * This demo shows the traditional (painful) approaches:
 * 1. DeserializationExceptionHandler - can only CONTINUE or FAIL
 * 2. try/catch in processor - manual error handling
 * 3. Manual routing to error topic using KafkaProducer
 *
 * Problems with this approach:
 * - No unified error handling
 * - Must manually create and manage KafkaProducer for error routing
 * - Error context/metadata must be manually captured
 * - Code duplication across processors
 */
public class BeforeDlqDemo {

    public static final String INPUT_TOPIC = "input-topic";
    public static final String OUTPUT_TOPIC = "output-topic";
    public static final String ERROR_TOPIC = "error-topic";  // Manual DLQ

    private static final int TOTAL_MESSAGES = 100;
    private static final double ERROR_RATE = 0.1;  // 10% error rate

    public static void main(String[] args) {
        System.out.println("========================================");
        System.out.println("  BEFORE DLQ Demo (Kafka 4.0.0)");
        System.out.println("  Manual Error Handling Example");
        System.out.println("========================================\n");

        // Step 1: Clean up topics (delete and recreate)
        cleanupTopics();

        // Step 2: Produce test messages
        produceTestMessages();

        // Step 3: Run the streams application
        runStreamsApp();
    }

    private static void cleanupTopics() {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        try (AdminClient admin = AdminClient.create(props)) {
            System.out.println("Cleaning up topics...");

            List<String> topics = Arrays.asList(INPUT_TOPIC, OUTPUT_TOPIC, ERROR_TOPIC);

            // Delete existing topics
            try {
                admin.deleteTopics(topics).all().get();
                System.out.println("  Deleted existing topics");
                Thread.sleep(1000);  // Wait for deletion to complete
            } catch (ExecutionException e) {
                // Topics might not exist, ignore
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }

            // Recreate topics
            List<NewTopic> newTopics = Arrays.asList(
                    new NewTopic(INPUT_TOPIC, 1, (short) 1),
                    new NewTopic(OUTPUT_TOPIC, 1, (short) 1),
                    new NewTopic(ERROR_TOPIC, 1, (short) 1)
            );
            admin.createTopics(newTopics).all().get();
            System.out.println("  Created fresh topics: " + topics);
            Thread.sleep(500);  // Wait for topics to be ready
        } catch (Exception e) {
            System.err.println("Warning: Topic cleanup failed: " + e.getMessage());
        }
        System.out.println();
    }

    private static void produceTestMessages() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            System.out.println("Producing " + TOTAL_MESSAGES + " test messages (~" +
                    (int)(ERROR_RATE * 100) + "% errors)...\n");

            Random random = new Random(42);  // Fixed seed for reproducibility
            int validCount = 0;
            int invalidCount = 0;

            String[] errorTypes = {
                    "INVALID_JSON",
                    "MALFORMED_DATA",
                    "CORRUPTED_PAYLOAD",
                    "<xml>wrong_format</xml>",
                    "null",
                    ""
            };

            for (int i = 1; i <= TOTAL_MESSAGES; i++) {
                String key = "key-" + i;
                String value;

                if (random.nextDouble() < ERROR_RATE) {
                    // Generate invalid message
                    value = errorTypes[random.nextInt(errorTypes.length)];
                    invalidCount++;
                } else {
                    // Generate valid JSON message
                    int amount = random.nextInt(1000) + 1;
                    String product = "product-" + (random.nextInt(10) + 1);
                    value = String.format("{\"id\": %d, \"product\": \"%s\", \"amount\": %d}",
                            i, product, amount);
                    validCount++;
                }

                producer.send(new ProducerRecord<>(INPUT_TOPIC, key, value));
            }

            producer.flush();
            System.out.println("Produced " + TOTAL_MESSAGES + " messages:");
            System.out.println("  - Valid: " + validCount);
            System.out.println("  - Invalid: " + invalidCount + " (will go to error topic)\n");
        }
    }

    private static void runStreamsApp() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "before-dlq-demo");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // Old way: DeserializationExceptionHandler only handles deserialization errors
        // It cannot route to DLQ, only CONTINUE (skip) or FAIL (crash)
//        props.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
//                LogAndContinueExceptionHandler.class);
        props.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
                LogAndFailExceptionHandler.class);
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> input = builder.stream(INPUT_TOPIC);

        // Create a shared producer for manual error routing (ugly!)
        KafkaProducer<String, String> errorProducer = createErrorProducer();

        // Manual try/catch in processor - very verbose!
        KStream<String, String> processed = input.mapValues((key, value) -> {
            try {
                // Simulate processing that might fail
                if (!value.startsWith("{")) {
                    throw new RuntimeException("Invalid JSON format: " + value);
                }

                // Process the message
                System.out.println("✓ Processing: key=" + key + ", value=" + value);
                return value.toUpperCase();

            } catch (Exception e) {
                // Manual error routing - lots of boilerplate!
                System.out.println("✗ Error processing key=" + key + ": " + e.getMessage());

                // Manually send to error topic
                String errorPayload = String.format(
                        "{\"originalKey\":\"%s\",\"originalValue\":\"%s\",\"error\":\"%s\"}",
                        key, value, e.getMessage()
                );
                errorProducer.send(new ProducerRecord<>(ERROR_TOPIC, key, errorPayload));

                return null;  // Return null to filter out
            }
        });

        // Filter out nulls (errors)
        processed.filter((k, v) -> v != null).to(OUTPUT_TOPIC);

        // Build and print topology (for https://zz85.github.io/kafka-streams-viz/)
        Topology topology = builder.build();
        System.out.println("=== TOPOLOGY (paste into kafka-streams-viz) ===");
        System.out.println(topology.describe());
        System.out.println("=== END TOPOLOGY ===\n");

        KafkaStreams streams = new KafkaStreams(topology, props);
        CountDownLatch latch = new CountDownLatch(1);

        // Live state listener - prints state changes in real-time
        streams.setStateListener((newState, oldState) -> {
            System.out.println("[STATE] " + oldState + " -> " + newState);
        });

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("\nShutting down...");
            streams.close(Duration.ofSeconds(5));
            errorProducer.close();
            latch.countDown();
        }));

        System.out.println("Starting Kafka Streams application...");
        System.out.println("Press Ctrl+C to stop\n");
        System.out.println("----------------------------------------");

        streams.start();

        try {
            latch.await();  // Run until Ctrl+C
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static KafkaProducer<String, String> createErrorProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return new KafkaProducer<>(props);
    }

    /**
     * Old-style deserialization handler - can only CONTINUE or FAIL
     * No option to route to DLQ!
     */
    public static class LogAndContinueExceptionHandler implements DeserializationExceptionHandler {
        @Override
        public DeserializationHandlerResponse handle(ProcessorContext context,
                                                      ConsumerRecord<byte[], byte[]> record,
                                                      Exception exception) {
            System.out.println("Deserialization error (old handler): " + exception.getMessage());
            // Can only: CONTINUE (skip) or FAIL (crash)
            // No DLQ option!
            return DeserializationHandlerResponse.CONTINUE;
        }

        @Override
        public void configure(Map<String, ?> configs) {}
    }

    /**
     * Old-style deserialization handler - can only CONTINUE or FAIL
     * No option to route to DLQ!
     */
    public static class LogAndFailExceptionHandler implements DeserializationExceptionHandler {
        @Override
        public DeserializationHandlerResponse handle(ProcessorContext context,
                                                     ConsumerRecord<byte[], byte[]> record,
                                                     Exception exception) {
            System.out.println("Deserialization error (old handler): " + exception.getMessage());
            return DeserializationHandlerResponse.FAIL;
        }

        @Override
        public void configure(Map<String, ?> configs) {}
    }
}
