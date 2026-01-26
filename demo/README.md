# KIP-1034 DLQ Demo

Before/After comparison of Kafka Streams error handling with DLQ.

## Quick Start

```bash
# 1. Start Kafka
docker-compose up -d

# 2. Create topics
docker exec kafka-dlq-demo /opt/kafka/bin/kafka-topics.sh \
  --create --topic input-topic --partitions 1 --replication-factor 1 \
  --bootstrap-server localhost:9092

docker exec kafka-dlq-demo /opt/kafka/bin/kafka-topics.sh \
  --create --topic output-topic --partitions 1 --replication-factor 1 \
  --bootstrap-server localhost:9092

docker exec kafka-dlq-demo /opt/kafka/bin/kafka-topics.sh \
  --create --topic dlq-topic --partitions 1 --replication-factor 1 \
  --bootstrap-server localhost:9092

# 3. Run demos (from kafka repo root)
cd ..
./gradlew :demo:run -PmainClass=demo.BeforeDlqDemo
./gradlew :demo:run -PmainClass=demo.AfterDlqDemo
```

## Produce Test Messages

```bash
# Normal message
echo "key1:valid-json" | docker exec -i kafka-dlq-demo \
  /opt/kafka/bin/kafka-console-producer.sh \
  --topic input-topic --bootstrap-server localhost:9092

# Bad message (to trigger error)
echo "key2:invalid{json" | docker exec -i kafka-dlq-demo \
  /opt/kafka/bin/kafka-console-producer.sh \
  --topic input-topic --bootstrap-server localhost:9092
```

## Consume Messages

```bash
# Output
docker exec kafka-dlq-demo /opt/kafka/bin/kafka-console-consumer.sh \
  --topic output-topic --from-beginning --bootstrap-server localhost:9092

# DLQ
docker exec kafka-dlq-demo /opt/kafka/bin/kafka-console-consumer.sh \
  --topic dlq-topic --from-beginning --bootstrap-server localhost:9092
```

## Cleanup

```bash
docker-compose down -v
```
