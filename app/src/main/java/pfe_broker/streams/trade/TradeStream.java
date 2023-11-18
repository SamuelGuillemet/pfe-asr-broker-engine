package pfe_broker.streams.trade;

import static pfe_broker.common.Log.APP;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import pfe_broker.avro.RejectedOrder;
import pfe_broker.avro.Trade;
import pfe_broker.config.BasicConfig;

public class TradeStream {
    private static final String APPLICATION_ID = "trade-integrity";

    private final TradeIntegrityCheckService integrityCheckService;

    private final SpecificAvroSerde<RejectedOrder> rejectedOrderAvroSerde;
    private final SpecificAvroSerde<Trade> tradeAvroSerde;
    private final Serde<Integer> keySerde;

    public TradeStream() {
        this.integrityCheckService = new TradeIntegrityCheckService();

        this.rejectedOrderAvroSerde = this.rejectedOrderAvroSerde();
        this.tradeAvroSerde = this.tradeAvroSerde();
        this.keySerde = Serdes.Integer();
    }

    protected Properties buildStreamsProperties() {
        Properties props = new Properties();

        props.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BasicConfig.BOOTSTRAP_SERVERS);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, BasicConfig.SCHEMA_REGISTRY_URL);

        return props;
    }

    private void createTopics() {
        Map<String, Object> config = new HashMap<>();

        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BasicConfig.BOOTSTRAP_SERVERS);
        AdminClient adminClient = AdminClient.create(config);

        List<NewTopic> topics = new ArrayList<>();

        topics.add(new NewTopic(BasicConfig.TRADES_TOPIC_NAME, 1, Short.parseShort("1")));
        topics.add(new NewTopic(BasicConfig.ACCEPTED_TRADES_TOPIC_NAME, 1, Short.parseShort("1")));

        adminClient.createTopics(topics);
        adminClient.close();
    }

    protected SpecificAvroSerde<Trade> tradeAvroSerde() {
        SpecificAvroSerde<Trade> orderAvroSerde = new SpecificAvroSerde<>();

        Map<String, String> serdeConfig = new HashMap<>();
        serdeConfig.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, BasicConfig.SCHEMA_REGISTRY_URL);
        orderAvroSerde.configure(serdeConfig, false);
        return orderAvroSerde;
    }

    protected SpecificAvroSerde<RejectedOrder> rejectedOrderAvroSerde() {
        SpecificAvroSerde<RejectedOrder> rejectedOrderAvroSerde = new SpecificAvroSerde<>();

        Map<String, String> serdeConfig = new HashMap<>();
        serdeConfig.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, BasicConfig.SCHEMA_REGISTRY_URL);
        rejectedOrderAvroSerde.configure(serdeConfig, false);
        return rejectedOrderAvroSerde;
    }

    protected Topology buildTopology() {
        StreamsBuilder builder = new StreamsBuilder();

        KStream<Integer, Trade> tradeStream = builder.stream(BasicConfig.TRADES_TOPIC_NAME,
                Consumed.with(this.keySerde, this.tradeAvroSerde));

        KStream<Integer, TradeIntegrityCheckRecord> integrityCheckedOrdersStream = tradeStream.mapValues(
                trade -> new TradeIntegrityCheckRecord(trade, integrityCheckService.checkIntegrity(trade)));

        processAcceptedAndRejectedTrades(integrityCheckedOrdersStream);

        return builder.build();
    }

    private void processAcceptedAndRejectedTrades(
            KStream<Integer, TradeIntegrityCheckRecord> integrityCheckedOrdersStream) {

        KStream<Integer, Trade> acceptedTrades = integrityCheckedOrdersStream
                .filter((key, value) -> value.orderRejectReason() == null)
                .mapValues(TradeIntegrityCheckRecord::trade);

        KStream<Integer, RejectedOrder> rejectedOrders = integrityCheckedOrdersStream
                .filter((key, value) -> value.orderRejectReason() != null)
                .mapValues(value -> new RejectedOrder(value.trade().getOrder(), value.orderRejectReason()));

        acceptedTrades.to(BasicConfig.ACCEPTED_TRADES_TOPIC_NAME, Produced.with(keySerde, this.tradeAvroSerde));
        rejectedOrders.to(BasicConfig.REJECTED_ORDERS_TOPIC_NAME, Produced.with(keySerde, this.rejectedOrderAvroSerde));
    }

    private void run() throws IOException {
        final Properties streamProps = this.buildStreamsProperties();

        Topology topology = this.buildTopology();
        this.createTopics();

        final KafkaStreams streams = new KafkaStreams(topology, streamProps);
        final CountDownLatch latch = new CountDownLatch(1);

        // Attach shutdown handler to catch Control-C.
        Runtime.getRuntime().addShutdownHook(new Thread("trade-integrity-shutdown-hook") {
            @Override
            public void run() {
                streams.close(Duration.ofSeconds(5));
                latch.countDown();
            }
        });

        try {
            APP.info("Starting trade integrity stream");
            streams.cleanUp();
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }

    public static void main(String[] args) throws IOException {
        new TradeStream().run();
    }
}
