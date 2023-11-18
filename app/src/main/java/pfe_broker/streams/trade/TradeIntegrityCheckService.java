package pfe_broker.streams.trade;

import static pfe_broker.common.Log.APP;
import static pfe_broker.common.Log.DB;

import java.util.Collections;
import java.util.List;

import pfe_broker.avro.OrderRejectReason;
import pfe_broker.avro.Side;
import pfe_broker.avro.Trade;
import pfe_broker.daos.UserDAO;
import pfe_broker.models.Stock;
import pfe_broker.models.User;

public class TradeIntegrityCheckService {
    private UserDAO userDAO;

    public TradeIntegrityCheckService() {
        this.userDAO = new UserDAO();
    }

    private void addStocks(User user, String symbol, Integer quantity) {
        // Create a list of length quantity with the same symbol
        Stock stock = new Stock();
        stock.setSymbol(symbol);

        List<Stock> stocks = Collections.nCopies(quantity, stock);

        DB.debug("Adding " + stocks.size() + " stocks of symbol " + symbol + "  to user " + user.getUsername());
        userDAO.addStocksToUser(user, stocks);
    }

    private void addAmountToUserBalance(User user, Double amount) {
        DB.debug("Adding " + amount + " to user " + user.getUsername());
        user.setBalance(user.getBalance() + amount);
        userDAO.update(user);
    }

    private void removeAmountFromUserBalance(User user, Double amount) {
        DB.debug("Removing " + amount + " from user " + user.getUsername());
        user.setBalance(user.getBalance() - amount);
        userDAO.update(user);
    }

    public OrderRejectReason checkIntegrity(Trade trade) {
        APP.info("Checking integrity of trade " + trade);
        String username = trade.getOrder().getUsername().toString();
        String symbol = trade.getSymbol().toString();
        Integer quantity = trade.getQuantity();
        Double price = trade.getPrice();
        Side side = trade.getOrder().getSide();
        Double amount = price * quantity;

        User user = userDAO.getByUsername(username);
        if (user == null) {
            APP.error("User " + username + " not found");
            return OrderRejectReason.BROKER_OPTION;
        }

        // This is applicable only for market orders
        if (side == Side.BUY) {
            if (user.getBalance() < amount) {
                APP.debug("Trade " + trade + " rejected because of insufficient balance, user " + username
                        + " has balance " + user.getBalance() + " and trade amount is " + amount);
                return OrderRejectReason.ORDER_EXCEEDS_LIMIT;
            }
            addStocks(user, symbol, quantity);
            removeAmountFromUserBalance(user, amount);
        } else {
            addAmountToUserBalance(user, amount);
        }

        APP.info("Trade " + trade + " accepted at " + java.time.Instant.now());

        return null;
    }
}
