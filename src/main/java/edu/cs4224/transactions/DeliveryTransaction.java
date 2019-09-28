package edu.cs4224.transactions;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Row;

import java.util.List;
import java.util.Optional;

public class DeliveryTransaction extends BaseTransaction {
  private static final String YET_DELIVERED_ORDER
      = "SELECT * FROM customer_order WHERE o_w_id = %d AND o_d_id = %d ORDER BY o_d_id, o_id LIMIT 1";
  private static final String UPDATE_CARRIER
      = "UPDATE customer_order SET o_carrier_id = %d WHERE o_w_id = %d AND o_d_id = %d AND o_id = %d";
  private static final String UPDATE_DELIVERY_DATE
      = "UPDATE order_line SET ol_delivery_d = toTimestamp(now()) WHERE ol_w_id = %d AND ol_d_id = %d AND ol_o_id = %d";
  private static final String ORDER_LINE_TOTAL_AMOUNT
      = "SELECT SUM(ol_amount) FROM order_line WHERE ol_w_id = %d AND ol_d_id = %d AND ol_o_id = %d";
  private static final String UPDATE_CUSTOMER
      = "UPDATE customer SET c_balance = c_balance + %f, c_delivery_cnt = c_delivery_cnt + 1 "
      + "WHERE c_w_id = %d AND c_d_id = %d AND c_id = %d";
  private static final int NUM_DISTRICTS = 10;

  private final int warehouseID;
  private final int carrierID;

  public DeliveryTransaction(final CqlSession session, final String[] parameters) {
    super(session, parameters);

    warehouseID = Integer.parseInt(parameters[1]);
    carrierID = Integer.parseInt(parameters[2]);
  }

  @Override public void execute(final String[] dataLines) {
    for (int i = 1; i <= NUM_DISTRICTS; i++) {
      // Finds the oldest yet-to-be-delivered order.
      String query = String.format(YET_DELIVERED_ORDER, warehouseID, i);
      List<Row> orders = executeQuery(query);
//      Optional<Row> yetDeliveredOrder = orders.stream().
//          filter(order -> order.isNull("o_carrier_id")).findFirst();
//      if (yetDeliveredOrder.isEmpty()) {
//        continue;
//      }
      Row yetDeliveredOrder = orders.get(0);

      // Updates the carrier.
      int orderID = yetDeliveredOrder.getInt("o_id");
      query = String.format(UPDATE_CARRIER, carrierID, warehouseID, i, orderID);
      executeQuery(query);

      // Updates the delivery date.
      query = String.format(UPDATE_DELIVERY_DATE, warehouseID, i, orderID);
      executeQuery(query);

      // Gets the total amount.
      query = String.format(ORDER_LINE_TOTAL_AMOUNT, warehouseID, i, orderID);
      double totalAmount = executeQuery(query).get(0).getDouble(0);

      // Updates the customer.
      int customerID = yetDeliveredOrder.getInt("o_c_id");
      query = String.format(UPDATE_CUSTOMER, totalAmount, warehouseID, i, customerID);
      executeQuery(query);
    }
  }
}
