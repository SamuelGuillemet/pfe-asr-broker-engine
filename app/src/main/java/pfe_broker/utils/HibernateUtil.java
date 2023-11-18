package pfe_broker.utils;

import pfe_broker.daos.StockDAO;
import pfe_broker.daos.UserDAO;
import pfe_broker.models.Stock;
import pfe_broker.models.User;

public class HibernateUtil {

    public static void clean() {
        UserDAO userDAO = new UserDAO();
        StockDAO stockDAO = new StockDAO();

        stockDAO.deleteMultiple(stockDAO.getAll());
        userDAO.deleteMultiple(userDAO.getAll());
    }

    public static void main(String[] args) {
        clean();
        UserDAO userDAO = new UserDAO();
        StockDAO stockDAO = new StockDAO();

        User user1 = new User("user1", "password1", "user1@localhost", 1000.);
        userDAO.create(user1);

        Stock stock1 = new Stock("AAPL", user1);
        Stock stock3 = new Stock("GOOG", user1);

        stockDAO.createN(stock1, 100);
        stockDAO.createN(stock3, 10);

    }
}
