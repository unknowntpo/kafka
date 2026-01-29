package org.apache.kafka.demo;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.DeserializationExceptionHandler;
import org.apache.kafka.streams.kstream.Consumed;
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
 * 2. No option to route to DLQ!
 */
public class BeforeDlqDemo {

    public static final String INPUT_TOPIC = "input-topic";
    public static final String OUTPUT_TOPIC = "output-topic";
    public static final String ERROR_TOPIC = "error-topic";

    private static final int TOTAL_MESSAGES = 100;
    private static final double ERROR_RATE = 0.1;

    public static void main(String[] args) {
        System.out.println("========================================");
        System.out.println("  BEFORE DLQ Demo (Kafka 4.0.0)");
        System.out.println("  DeserializationExceptionHandler Demo");
        System.out.println("========================================\n");

        cleanupTopics();
        produceTestMessages();
        runStreamsApp();
    }

    private static void cleanupTopics() {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        try (AdminClient admin = AdminClient.create(props)) {
            System.out.println("Cleaning up topics...");
            List<String> topics = Arrays.asList(INPUT_TOPIC, OUTPUT_TOPIC, ERROR_TOPIC);

            try {
                admin.deleteTopics(topics).all().get();
                Thread.sleep(1000);
            } catch (ExecutionException | InterruptedException e) {
                // ignore
            }

            List<NewTopic> newTopics = Arrays.asList(
                    new NewTopic(INPUT_TOPIC, 1, (short) 1),
                    new NewTopic(OUTPUT_TOPIC, 1, (short) 1),
                    new NewTopic(ERROR_TOPIC, 1, (short) 1)
            );
            admin.createTopics(newTopics).all().get();
            System.out.println("  Created topics: " + topics);
            Thread.sleep(500);
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
            System.out.println("Producing " + TOTAL_MESSAGES + " messages (~" +
                    (int)(ERROR_RATE * 100) + "% invalid JSON)...\n");

            Random random = new Random(42);
            int validCount = 0;
            int invalidCount = 0;

            String[] invalidValues = {"INVALID_JSON", "not_json", "{malformed"};

            for (int i = 1; i <= TOTAL_MESSAGES; i++) {
                String key = "key-" + i;
                String value;

                if (random.nextDouble() < ERROR_RATE) {
                    value = invalidValues[random.nextInt(invalidValues.length)];
                    invalidCount++;
                } else {
                    int amount = random.nextInt(1000) + 1;
                    String product = "product-" + (random.nextInt(10) + 1);
                    value = String.format("{\"id\": %d, \"product\": \"%s\", \"amount\": %d}",
                            i, product, amount);
                    validCount++;
                }

                producer.send(new ProducerRecord<>(INPUT_TOPIC, key, value));
            }

            producer.flush();
            System.out.println("Produced: " + validCount + " valid JSON, " + invalidCount + " invalid JSON\n");
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
        props.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
                LogAndContinueExceptionHandler.class);
//        props.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
//                LogAndFailExceptionHandler.class);

        StreamsBuilder builder = new StreamsBuilder();

        // Use JsonSerde - invalid JSON will trigger DeserializationExceptionHandler
        Serde<JsonNode> jsonSerde = new JsonSerde();
        KStream<String, JsonNode> input = builder.stream(INPUT_TOPIC,
                Consumed.with(Serdes.String(), jsonSerde));

        // Process valid JSON messages
        KStream<String, String> processed = input.mapValues((key, jsonNode) -> {
            System.out.println("✓ Processing: key=" + key + ", json=" + jsonNode);
            return jsonNode.toString().toUpperCase();
        });

        processed.to(OUTPUT_TOPIC);

        Topology topology = builder.build();
        System.out.println("=== TOPOLOGY ===");
        System.out.println(topology.describe());
        System.out.println("================\n");

        KafkaStreams streams = new KafkaStreams(topology, props);
        CountDownLatch latch = new CountDownLatch(1);

        streams.setStateListener((newState, oldState) ->
            System.out.println("[STATE] " + oldState + " -> " + newState));

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("\nShutting down...");
            streams.close(Duration.ofSeconds(5));
            latch.countDown();
        }));

        System.out.println("Starting Kafka Streams with JSON Serde...");
        System.out.println("Invalid JSON will trigger DeserializationExceptionHandler");
        System.out.println("Press Ctrl+C to stop\n");
        System.out.println("----------------------------------------");

        streams.start();

        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * JSON Serde using Jackson - invalid JSON triggers deserialization error
     */
    public static class JsonSerde implements Serde<JsonNode> {
        private static final ObjectMapper MAPPER = new ObjectMapper();

        @Override
        public Serializer<JsonNode> serializer() {
            return (topic, data) -> {
                try {
                    return MAPPER.writeValueAsBytes(data);
                } catch (Exception e) {
                    throw new RuntimeException("Failed to serialize", e);
                }
            };
        }

        @Override
        public Deserializer<JsonNode> deserializer() {
            return (topic, data) -> {
                try {
                    return MAPPER.readTree(data);
                } catch (Exception e) {
                    throw new RuntimeException("Invalid JSON: " + new String(data), e);
                }
            };
        }
    }

    /**
     * Old-style handler - CONTINUE (skip record)
     * No DLQ option!
     */
    public static class LogAndContinueExceptionHandler implements DeserializationExceptionHandler {
        @Override
        public DeserializationHandlerResponse handle(ProcessorContext context,
                                                      ConsumerRecord<byte[], byte[]> record,
                                                      Exception exception) {
            System.out.println("⚠ Deserialization error: " + exception.getMessage());
            System.out.println("  -> Skipping record, NO DLQ!");
            return DeserializationHandlerResponse.CONTINUE;
        }

        @Override
        public void configure(Map<String, ?> configs) {}
    }

    /**
     * Old-style handler - FAIL (crash app)
     * No DLQ option!
     */
    public static class LogAndFailExceptionHandler implements DeserializationExceptionHandler {
        @Override
        public DeserializationHandlerResponse handle(ProcessorContext context,
                                                     ConsumerRecord<byte[], byte[]> record,
                                                     Exception exception) {
            System.out.println("✗ Deserialization error: " + exception.getMessage());
            System.out.println("  -> FAILING! NO DLQ!");
            return DeserializationHandlerResponse.FAIL;
        }

        @Override
        public void configure(Map<String, ?> configs) {}
    }
}