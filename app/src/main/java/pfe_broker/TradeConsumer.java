package pfe_broker;

import static pfe_broker.common.Log.APP;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.IntegerDeserializer;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import pfe_broker.avro.Trade;
import pfe_broker.config.BasicConfig;

public class TradeConsumer {
    private static final String GROUP_ID_DATA = "mock-trade-consumer";

    protected Properties consumerProperties() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BasicConfig.BOOTSTRAP_SERVERS);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
        properties.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);
        properties.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, BasicConfig.SCHEMA_REGISTRY_URL);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID_DATA);
        return properties;
    }

    private void startTradeConsumer(Consumer<Integer, Trade> consumer) {
        while (true) {
            ConsumerRecords<Integer, Trade> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<Integer, Trade> record : records) {
                System.out.printf("Received trade: %s at instant %s%n", record.value(), java.time.Instant.now());
            }
        }
    }

    private void run() {
        List<String> topics = Arrays.asList(BasicConfig.ACCEPTED_TRADES_TOPIC_NAME);

        Consumer<Integer, Trade> consumer = new KafkaConsumer<>(consumerProperties());
        consumer.subscribe(topics);

        Runtime.getRuntime().addShutdownHook(new Thread("market-matcher-shutdown-hook") {
            @Override
            public void run() {
                consumer.close(Duration.ofSeconds(5));
            }
        });

        APP.info("Starting consumer...");
        startTradeConsumer(consumer);
    }

    public static void main(String[] args) {
        new TradeConsumer().run();
    }
}
