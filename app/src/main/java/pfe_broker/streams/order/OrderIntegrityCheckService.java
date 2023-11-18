package pfe_broker.streams.order;

import static pfe_broker.common.Log.APP;
import static pfe_broker.common.Log.DB;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.streams.StreamsConfig;

import pfe_broker.avro.Order;
import pfe_broker.avro.OrderRejectReason;
import pfe_broker.avro.Side;
import pfe_broker.config.BasicConfig;
import pfe_broker.daos.StockDAO;
import pfe_broker.daos.UserDAO;
import pfe_broker.models.Stock;
import pfe_broker.models.User;

public class OrderIntegrityCheckService {
    private final UserDAO userDAO;
    private final StockDAO stockDAO;
    private List<String> symbols;

    public OrderIntegrityCheckService() {
        this.userDAO = new UserDAO();
        this.stockDAO = new StockDAO();
        try {
            this.symbols = listAvailableSymbols();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
            this.symbols = new ArrayList<>();
        }
    }

    private List<String> listAvailableSymbols() throws InterruptedException, ExecutionException {
        Map<String, Object> config = new HashMap<>();

        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BasicConfig.BOOTSTRAP_SERVERS);
        AdminClient client = AdminClient.create(config);
        Collection<String> topics = client.listTopics().names().get();
        return topics.stream().filter(s -> s.startsWith(BasicConfig.SYMBOL_TOPIC_PREFIX))
                .map(s -> s.substring(BasicConfig.SYMBOL_TOPIC_PREFIX.length()))
                .toList();
    }

    public void consumeStocks(User user, List<Stock> stocks) {
        DB.debug("Consuming " + stocks.size() + " stocks of symbol " + stocks.get(0).getSymbol() + " from user "
                + user.getUsername());
        stockDAO.deleteMultiple(stocks);
    }

    public OrderRejectReason checkIntegrity(Order order) {
        APP.info("Checking integrity of order " + order);
        String username = order.getUsername().toString();
        String symbol = order.getSymbol().toString();
        Integer quantity = order.getQuantity();
        Side side = order.getSide();

        if (username == null || username.isEmpty()) {
            APP.debug("Order " + order + " rejected because of empty username");
            return OrderRejectReason.BROKER_OPTION;
        }
        User user = userDAO.getByUsername(username);
        if (user == null) {
            APP.debug("Order " + order + " rejected because of unknown user");
            return OrderRejectReason.BROKER_OPTION;
        }
        if (symbol == null || symbol.isEmpty() || !symbols.contains(symbol)) {
            APP.debug("Order " + order + " rejected because of unknown symbol");
            return OrderRejectReason.UNKNOWN_SYMBOL;
        }
        if (quantity == null || quantity <= 0) {
            APP.debug("Order " + order + " rejected because of invalid quantity");
            return OrderRejectReason.ORDER_EXCEEDS_LIMIT;
        }

        if (side == Side.SELL) {
            List<Stock> stocks = userDAO.getStocksBySymbol(user, symbol);
            if (stocks == null) {
                APP.debug("Order " + order + " rejected because of unknown user");
                return OrderRejectReason.ORDER_EXCEEDS_LIMIT;
            }
            if (stocks.size() < quantity) {
                APP.debug("Order " + order + " rejected because of insufficient stocks");
                return OrderRejectReason.ORDER_EXCEEDS_LIMIT;
            }
            APP.debug("User " + username + " has " + stocks.size() + " stocks of " + symbol);
            // Remove the stocks from the user
            consumeStocks(user, stocks.subList(0, quantity));
        }

        APP.info("Order " + order + " accepted at " + java.time.Instant.now());

        return null;
    }
}
