package pfe_broker.config;

public class BasicConfig {
    public static final String SYMBOL_TOPIC_PREFIX = "market-data.";

    public static final String ACCEPTED_ORDERS_TOPIC_NAME = "accepted-orders";
    public static final String REJECTED_ORDERS_TOPIC_NAME = "rejected-orders";
    public static final String ORDERS_TOPIC_NAME = "orders";
    public static final String ACCEPTED_TRADES_TOPIC_NAME = "accepted-trades";
    public static final String TRADES_TOPIC_NAME = "trades";

    public static final String SCHEMA_REGISTRY_URL = "http://localhost:8081";
    public static final String BOOTSTRAP_SERVERS = "localhost:9092";
}
