package pfe_broker.daos;

import static pfe_broker.common.Log.DB;

import java.util.List;

import pfe_broker.models.Stock;
import pfe_broker.models.User;

public class UserDAO extends GenericDAO<User> {
    private StockDAO stockDAO = new StockDAO();

    public UserDAO() {
        super(User.class);
    }

    public User getByUsername(String username) {
        try {
            List<User> users = getByAttribute("username", username);
            if (users.isEmpty()) {
                DB.error("User " + username + " not found");
                return null;
            }
            return users.get(0);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public List<Stock> getStocksBySymbol(User user, String symbol) {
        return user.getStocks().stream().filter(stock -> stock.getSymbol().equals(symbol)).toList();
    }

    public void addStocksToUser(User user, List<Stock> stocks) {
        for (Stock stock : stocks) {
            stock.setUser(user);
            stockDAO.create(stock);
        }
    }
}
