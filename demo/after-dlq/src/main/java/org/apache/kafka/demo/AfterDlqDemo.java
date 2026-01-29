package org.apache.kafka.demo;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.DeserializationExceptionHandler;
import org.apache.kafka.streams.errors.ErrorHandlerContext;
import org.apache.kafka.streams.errors.ProcessingExceptionHandler;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.processor.api.Record;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;

/**
 * Demo: Error handling AFTER KIP-1034 DLQ
 *
 * This demo shows the new unified DLQ approach:
 * 1. DeserializationExceptionHandler.handleError() - can route to DLQ
 * 2. ProcessingExceptionHandler.handleError() - can route to DLQ
 * 3. Both return Response with deadLetterQueueRecords
 * 4. Automatic error metadata in headers
 *
 * Benefits:
 * - Unified error handling interface
 * - No need for manual KafkaProducer
 * - Rich error metadata in DLQ headers
 * - Clean separation of concerns
 */
public class AfterDlqDemo {

    public static final String INPUT_TOPIC = "input-topic";
    public static final String OUTPUT_TOPIC = "output-topic";
    public static final String DLQ_TOPIC = "dlq-topic";  // Dead Letter Queue

    private static final int TOTAL_MESSAGES = 100;
    private static final double ERROR_RATE = 0.1;  // 10% error rate

    public static void main(String[] args) {
        System.out.println("========================================");
        System.out.println("  AFTER DLQ Demo (Kafka 4.3.0-SNAPSHOT)");
        System.out.println("  KIP-1034 DLQ Example");
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

            List<String> topics = Arrays.asList(INPUT_TOPIC, OUTPUT_TOPIC, DLQ_TOPIC);

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
                    new NewTopic(DLQ_TOPIC, 1, (short) 1)
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
            System.out.println("  - Invalid: " + invalidCount + " (will go to DLQ)\n");
        }
    }

    private static void runStreamsApp() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "after-dlq-demo");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // NEW! Configure ProcessingExceptionHandler for processing errors
        props.put(StreamsConfig.PROCESSING_EXCEPTION_HANDLER_CLASS_CONFIG,
                DlqProcessingExceptionHandler.class);

        // NEW! Configure DeserializationExceptionHandler for deserialization errors
        props.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
                DlqDeserializationExceptionHandler.class);

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> input = builder.stream(INPUT_TOPIC);

        // Clean processing logic - no try/catch needed!
        KStream<String, String> processed = input.mapValues((key, value) -> {
            // Simulate processing that might fail
            if (!value.startsWith("{")) {
                // Just throw - the handler will route to DLQ
                throw new RuntimeException("Invalid JSON format: " + value);
            }

            // Process the message
            System.out.println("✓ Processing: key=" + key + ", value=" + value);
            return value.toUpperCase();
        });

        processed.to(OUTPUT_TOPIC);

        // Build and print topology (for https://zz85.github.io/kafka-streams-viz/)
        Topology topology = builder.build();
        System.out.println("=== TOPOLOGY (paste into kafka-streams-viz) ===");
        System.out.println(topology.describe());
        System.out.println("=== END TOPOLOGY ===\n");

        KafkaStreams streams = new KafkaStreams(topology, props);
        CountDownLatch latch = new CountDownLatch(1);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("\nCtrl+C caught, shutting down...");
            streams.close(Duration.ofSeconds(5));
            latch.countDown();
        }));

        System.out.println("Starting Kafka Streams application...");
        System.out.println("Press Ctrl+C to stop\n");
        System.out.println("----------------------------------------");

        streams.start();

        try {
            // wait forever until Ctrl+C
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        System.out.println("----------------------------------------\n");
        System.out.println("Benefits of KIP-1034 DLQ approach:");
        System.out.println("  1. No manual KafkaProducer needed");
        System.out.println("  2. Clean processing code (no try/catch)");
        System.out.println("  3. Rich error metadata in DLQ headers");
        System.out.println("  4. Unified handler for all error types");
        System.out.println("  5. Easy to inspect and replay failed messages\n");

        streams.close();
    }

    /**
     * NEW! ProcessingExceptionHandler with DLQ support
     *
     * Handles errors that occur during message processing (after deserialization).
     * Can route failed messages to a Dead Letter Queue with rich metadata.
     */
    public static class DlqProcessingExceptionHandler implements ProcessingExceptionHandler {

        @Override
        public Response handleError(ErrorHandlerContext context,
                                    Record<?, ?> record,
                                    Exception exception) {
            System.out.println("✗ Processing error: " + exception.getMessage());
            System.out.println("  Topic: " + context.topic() +
                    ", Partition: " + context.partition() +
                    ", Offset: " + context.offset());

            // Create DLQ record with error metadata in headers
            List<Header> headers = new ArrayList<>();
            headers.add(new RecordHeader("error.message",
                    exception.getMessage().getBytes(StandardCharsets.UTF_8)));
            headers.add(new RecordHeader("error.class",
                    exception.getClass().getName().getBytes(StandardCharsets.UTF_8)));
            headers.add(new RecordHeader("source.topic",
                    context.topic().getBytes(StandardCharsets.UTF_8)));
            headers.add(new RecordHeader("source.partition",
                    String.valueOf(context.partition()).getBytes(StandardCharsets.UTF_8)));
            headers.add(new RecordHeader("source.offset",
                    String.valueOf(context.offset()).getBytes(StandardCharsets.UTF_8)));
            headers.add(new RecordHeader("processor.node",
                    context.processorNodeId().getBytes(StandardCharsets.UTF_8)));

            // Convert stacktrace to string
            java.io.StringWriter sw = new java.io.StringWriter();
            exception.printStackTrace(new java.io.PrintWriter(sw));
            headers.add(new RecordHeader("error.stacktrace",
                    sw.toString().getBytes(StandardCharsets.UTF_8)));

            // Create the DLQ record
            ProducerRecord<byte[], byte[]> dlqRecord = new ProducerRecord<>(
                    DLQ_TOPIC,
                    null,  // partition
                    context.sourceRawKey(),
                    context.sourceRawValue(),
                    headers
            );

            System.out.println("  -> Routed to DLQ: " + DLQ_TOPIC);

            // Resume processing and send to DLQ
            return Response.resume(Collections.singletonList(dlqRecord));
        }

        @Override
        public void configure(Map<String, ?> configs) {
            // Configuration if needed
        }
    }

    /**
     * NEW! DeserializationExceptionHandler with DLQ support
     *
     * Handles errors that occur during message deserialization.
     * Can route failed messages to a Dead Letter Queue with rich metadata.
     */
    public static class DlqDeserializationExceptionHandler implements DeserializationExceptionHandler {

        @Override
        public Response handleError(ErrorHandlerContext context,
                                    ConsumerRecord<byte[], byte[]> record,
                                    Exception exception) {
            System.out.println("✗ Deserialization error: " + exception.getMessage());
            System.out.println("  Topic: " + record.topic() +
                    ", Partition: " + record.partition() +
                    ", Offset: " + record.offset());

            // Create DLQ record with error metadata in headers
            List<Header> headers = new ArrayList<>();
            headers.add(new RecordHeader("error.message",
                    exception.getMessage().getBytes(StandardCharsets.UTF_8)));
            headers.add(new RecordHeader("error.class",
                    exception.getClass().getName().getBytes(StandardCharsets.UTF_8)));
            headers.add(new RecordHeader("error.type",
                    "deserialization".getBytes(StandardCharsets.UTF_8)));
            headers.add(new RecordHeader("source.topic",
                    record.topic().getBytes(StandardCharsets.UTF_8)));
            headers.add(new RecordHeader("source.partition",
                    String.valueOf(record.partition()).getBytes(StandardCharsets.UTF_8)));
            headers.add(new RecordHeader("source.offset",
                    String.valueOf(record.offset()).getBytes(StandardCharsets.UTF_8)));

            // Convert stacktrace to string
            java.io.StringWriter sw = new java.io.StringWriter();
            exception.printStackTrace(new java.io.PrintWriter(sw));
            headers.add(new RecordHeader("error.stacktrace",
                    sw.toString().getBytes(StandardCharsets.UTF_8)));

            // Create the DLQ record
            ProducerRecord<byte[], byte[]> dlqRecord = new ProducerRecord<>(
                    DLQ_TOPIC,
                    null,  // partition
                    record.key(),
                    record.value(),
                    headers
            );

            System.out.println("  -> Routed to DLQ: " + DLQ_TOPIC);

            // Resume processing and send to DLQ
            return Response.resume(Collections.singletonList(dlqRecord));
        }

        @Override
        public void configure(Map<String, ?> configs) {
            // Configuration if needed
        }
    }
}
