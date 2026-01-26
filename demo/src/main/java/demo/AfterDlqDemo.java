package demo;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

/**
 * Demo: Error handling AFTER KIP-1034 DLQ
 *
 * Shows new DLQ approach:
 * - ProcessingExceptionHandler
 * - Automatic DLQ routing
 * - Error metadata in headers
 *
 * TODO: Implement your demo logic here
 */
public class AfterDlqDemo {

    public static final String INPUT_TOPIC = "input-topic";
    public static final String OUTPUT_TOPIC = "output-topic";
    public static final String DLQ_TOPIC = "dlq-topic";

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "after-dlq-demo");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // TODO: Configure ProcessingExceptionHandler for DLQ
        // props.put(StreamsConfig.PROCESSING_EXCEPTION_HANDLER_CLASS_CONFIG, ...);

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> input = builder.stream(INPUT_TOPIC);

        // TODO: Add your processing logic here
        // Show how DLQ simplifies error handling

        input.to(OUTPUT_TOPIC);

        KafkaStreams streams = new KafkaStreams(builder.build(), props);

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

        System.out.println("Starting AfterDlqDemo...");
        streams.start();
    }
}
