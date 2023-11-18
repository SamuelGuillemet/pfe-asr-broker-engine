package pfe_broker.daos;

import pfe_broker.models.Stock;

public class StockDAO extends GenericDAO<Stock> {
    public StockDAO() {
        super(Stock.class);
    }

    public Stock createN(Stock stock, int n) {
        for (int i = 0; i < n; i++) {
            create(stock);
        }
        return stock;
    }
}
