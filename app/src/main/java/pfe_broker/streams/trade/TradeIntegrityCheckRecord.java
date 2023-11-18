package pfe_broker.streams.trade;

import pfe_broker.avro.OrderRejectReason;
import pfe_broker.avro.Trade;

public record TradeIntegrityCheckRecord(Trade trade, OrderRejectReason orderRejectReason) {
}
