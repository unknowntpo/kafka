# KIP-1034 DLQ Demo Commands

# Default recipe
default:
    @just --list

# Start Kafka with docker-compose
up:
    docker compose -f demo/docker-compose.yml up -d kafka
    @echo "Waiting for Kafka to be ready..."
    @sleep 5
    @echo "Kafka is ready at localhost:9092"

# Stop Kafka
down:
    docker compose -f demo/docker-compose.yml down

# Show Kafka logs
logs:
    docker compose -f demo/docker-compose.yml logs -f kafka

# Run BEFORE DLQ demo (Kafka 4.0.0 - shows limitation)
before:
    ./gradlew :demo:before-dlq:run --console=plain

# Run AFTER DLQ demo (Kafka 4.2.0 - shows KIP-1034)
after:
    ./gradlew :demo:after-dlq:run --console=plain

# Build both demos
build:
    ./gradlew :demo:before-dlq:compileJava :demo:after-dlq:compileJava

# Clean build artifacts
clean:
    ./gradlew :demo:before-dlq:clean :demo:after-dlq:clean

# List topics
topics:
    docker exec kafka-dlq-demo /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list

# Consume from input-topic
consume-input:
    docker exec kafka-dlq-demo /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic input-topic --from-beginning

# Consume from output-topic
consume-output:
    docker exec kafka-dlq-demo /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic output-topic --from-beginning

# Consume from DLQ topic (after-dlq demo)
consume-dlq:
    docker exec kafka-dlq-demo /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic dlq-topic --from-beginning --property print.headers=true

# Consume from error-topic (before-dlq demo)
consume-error:
    docker exec kafka-dlq-demo /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic error-topic --from-beginning

# Delete all demo topics
delete-topics:
    docker exec kafka-dlq-demo /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic input-topic || true
    docker exec kafka-dlq-demo /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic output-topic || true
    docker exec kafka-dlq-demo /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic error-topic || true
    docker exec kafka-dlq-demo /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic dlq-topic || true

# Full demo setup: start kafka, build demos
setup: up build
    @echo "Setup complete! Run 'just before' or 'just after' to start demos"