package pfe_broker.streams.order;

import pfe_broker.avro.Order;
import pfe_broker.avro.OrderRejectReason;

public record OrderIntegrityCheckRecord(Order order, OrderRejectReason orderRejectReason) {
}
