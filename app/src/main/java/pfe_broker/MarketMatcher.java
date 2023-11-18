package pfe_broker;

import static pfe_broker.common.Log.APP;

import java.io.IOException;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import pfe_broker.avro.MarketData;
import pfe_broker.avro.Order;
import pfe_broker.avro.Trade;
import pfe_broker.config.BasicConfig;

public class MarketMatcher {
    private static final String GROUP_ID_DATA = "market-matcher-data";
    private static final String GROUP_ID_ORDERS = "market-matcher-orders";

    private Callback callback = new Callback() {
        @Override
        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
            if (e == null) {
                return;
            } else {
                e.printStackTrace();
            }
        }
    };

    protected Properties producerProperties() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BasicConfig.BOOTSTRAP_SERVERS);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, BasicConfig.SCHEMA_REGISTRY_URL);
        return properties;
    }

    protected Properties consumerGeneralProperties() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BasicConfig.BOOTSTRAP_SERVERS);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        properties.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);
        properties.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, BasicConfig.SCHEMA_REGISTRY_URL);
        return properties;
    }

    protected Properties marketDataProperties() {
        Properties properties = consumerGeneralProperties();
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID_DATA);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        return properties;
    }

    protected Properties ordersProperties() {
        Properties properties = consumerGeneralProperties();
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID_ORDERS);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
        return properties;
    }

    private void startMatchingEngine(
            Consumer<String, MarketData> marketDataConsumer,
            Consumer<Integer, Order> ordersConsumer,
            Producer<String, Trade> tradesProducer,
            List<String> symbols) {
        while (true) {
            ConsumerRecords<Integer, Order> records = ordersConsumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<Integer, Order> record : records) {
                String symbol = record.value().getSymbol().toString();

                if (!symbols.contains(symbol)) {
                    APP.debug("Symbol " + symbol + " is not available");
                }

                MarketData stockData = readLastStockData(symbol, marketDataConsumer);

                if (stockData == null) {
                    // Should never happen
                    APP.fatal("No market data for symbol " + symbol);
                    continue;
                }

                APP.info("Matching order " + record.value() + " with market data " + stockData + "at instant "
                        + java.time.Instant.now());

                Trade trade = Trade.newBuilder()
                        .setOrder(record.value())
                        .setPrice(stockData.getClose())
                        .setSymbol(symbol)
                        .setQuantity(record.value().getQuantity())
                        .build();

                tradesProducer.send(new ProducerRecord<>(BasicConfig.TRADES_TOPIC_NAME, trade), callback);

                // Manually commit the offset for the record
                Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
                offsets.put(new TopicPartition(record.topic(), record.partition()),
                        new OffsetAndMetadata(record.offset() + 1)); // +1 because offsets are zero-based

                ordersConsumer.commitSync(offsets);
            }
        }
    }

    private MarketData readLastStockData(String symbol, Consumer<String, MarketData> marketDataConsumer) {
        List<TopicPartition> partitions = marketDataConsumer.partitionsFor(BasicConfig.SYMBOL_TOPIC_PREFIX + symbol)
                .stream()
                .map(partitionInfo -> new TopicPartition(partitionInfo.topic(), partitionInfo.partition()))
                .toList();

        marketDataConsumer.assign(partitions);
        marketDataConsumer.seekToEnd(partitions);

        for (TopicPartition partition : partitions) {
            long offset = marketDataConsumer.position(partition) - 1;
            marketDataConsumer.seek(partition, offset);
        }

        ConsumerRecords<String, MarketData> records = marketDataConsumer.poll(Duration.ofMillis(100));

        ConsumerRecord<String, MarketData> stockData = null;
        for (ConsumerRecord<String, MarketData> record : records) {
            if (stockData == null || record.timestamp() > stockData.timestamp()) {
                stockData = record;
            }
        }

        return stockData == null ? null : stockData.value();
    }

    private void run() {
        Consumer<String, MarketData> marketDataConsumer = new KafkaConsumer<>(marketDataProperties());
        Consumer<Integer, Order> ordersConsumer = new KafkaConsumer<>(ordersProperties());
        ordersConsumer.subscribe(Collections.singletonList(BasicConfig.ACCEPTED_ORDERS_TOPIC_NAME));

        Producer<String, Trade> tradesProducer = new KafkaProducer<>(producerProperties());

        List<String> symbols = listAvailableSymbols();

        // Attach shutdown handler to catch Control-C.
        Runtime.getRuntime().addShutdownHook(new Thread("market-matcher-shutdown-hook") {
            @Override
            public void run() {
                marketDataConsumer.close(Duration.ofSeconds(5));
                ordersConsumer.close(Duration.ofSeconds(5));
                tradesProducer.close(Duration.ofSeconds(5));
            }
        });

        APP.info("Starting market matcher");
        startMatchingEngine(
                marketDataConsumer,
                ordersConsumer,
                tradesProducer,
                symbols);
    }

    private List<String> listAvailableSymbols() {
        Properties properties = marketDataProperties();
        Consumer<String, MarketData> consumer = new KafkaConsumer<>(properties);
        Collection<String> topics = consumer.listTopics().keySet();
        consumer.close();
        return topics.stream().filter(s -> s.startsWith(BasicConfig.SYMBOL_TOPIC_PREFIX))
                .map(s -> s.substring(BasicConfig.SYMBOL_TOPIC_PREFIX.length()))
                .toList();
    }

    public static void main(String[] args) throws IOException {
        new MarketMatcher().run();
    }
}
