package pfe_broker;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.streams.StreamsConfig;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import pfe_broker.avro.Order;
import pfe_broker.avro.Side;
import pfe_broker.config.BasicConfig;

public class OrderProducer {

    protected Properties buildStreamsProperties() {
        Properties props = new Properties();

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BasicConfig.BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, BasicConfig.SCHEMA_REGISTRY_URL);

        return props;
    }

    protected List<String> getTopics() throws InterruptedException, ExecutionException {
        Map<String, Object> config = new HashMap<>();

        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BasicConfig.BOOTSTRAP_SERVERS);
        AdminClient client = AdminClient.create(config);
        List<String> topics = client.listTopics().names().get().stream()
                .filter(s -> s.startsWith(BasicConfig.SYMBOL_TOPIC_PREFIX))
                .collect(Collectors.toList());
        return topics;
    }

    protected List<Order> generateOrders() throws InterruptedException, ExecutionException {
        List<String> topics = getTopics();

        List<Order> orders = new ArrayList<>();

        // Produce 100 correct orders
        for (int i = 0; i < 100; i++) {
            Side side = i % 2 == 0 ? Side.BUY : Side.SELL;
            Integer quantity = (i % 10) + 1;
            String symbol = topics.get(i % topics.size()).substring(BasicConfig.SYMBOL_TOPIC_PREFIX.length());
            String choosen_symbol = i % 3 != 0 ? symbol : "AAPL";
            orders.add(new Order("user1", choosen_symbol, quantity, side));
        }

        //Produce 10 incorrect orders
        for (int i = 0; i < 10; i++) {
            Side side = i % 2 == 0 ? Side.BUY : Side.SELL;
            Integer quantity = (i % 10) + 1;
            orders.add(new Order("user1", "", quantity, side));
        }

        // Mix the orders
        Collections.shuffle(orders);

        return orders;
    }

    private Callback callback = new Callback() {
        @Override
        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
            if (e == null) {
                java.time.Instant instant = java.time.Instant.now();
                System.out.printf("produced at %s%n", instant);
            } else {
                e.printStackTrace();
            }
        }
    };

    public void run() throws InterruptedException, ExecutionException {
        Producer<Integer, Order> producer = new KafkaProducer<>(buildStreamsProperties());

        List<Order> orders = generateOrders();

        for (int i = 0; i < orders.size(); i++) {
            Order order = orders.get(i);
            ProducerRecord<Integer, Order> record = new ProducerRecord<>(BasicConfig.ORDERS_TOPIC_NAME, i, order);
            producer.send(record, callback);
            System.out.printf("Order %s ", order);
            Thread.sleep(3000);
        }

        producer.close();
    }

    public static void main(String[] args) throws InterruptedException, ExecutionException {
        new OrderProducer().run();
    }
}