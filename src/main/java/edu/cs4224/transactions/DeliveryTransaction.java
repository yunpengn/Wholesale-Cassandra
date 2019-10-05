package edu.cs4224.transactions;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Row;

import java.util.List;

public class DeliveryTransaction extends BaseTransaction {
  public static final String YET_DELIVERED_ORDER
      = "SELECT * FROM customer_order WHERE o_w_id = %d AND o_d_id = %d ORDER BY o_d_id, o_id";
  private static final String UPDATE_CARRIER
      = "UPDATE customer_order SET o_carrier_id = %d WHERE o_w_id = %d AND o_d_id = %d AND o_id = %d";
  private static final String GET_ORDER_LINE_FROM_ORDER
      = "SELECT ol_number FROM order_line WHERE ol_w_id = %d AND ol_d_id = %d AND ol_o_id = %d";
  private static final String UPDATE_DELIVERY_DATE
      = "UPDATE order_line SET ol_delivery_d = toTimestamp(now()) WHERE ol_w_id = %d AND ol_d_id = %d AND ol_o_id = %d "
      + "AND ol_number IN (%s)";
  private static final String ORDER_LINE_TOTAL_AMOUNT
      = "SELECT SUM(ol_amount) FROM order_line WHERE ol_w_id = %d AND ol_d_id = %d AND ol_o_id = %d";
  private static final String GET_CUSTOMER
      = "SELECT c_balance, c_delivery_cnt FROM customer WHERE c_w_id = %d AND c_d_id = %d AND c_id = %d";
  private static final String UPDATE_CUSTOMER
      = "UPDATE customer SET c_balance = %f, c_delivery_cnt = %d WHERE c_w_id = %d AND c_d_id = %d AND c_id = %d";
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
      Row yetDeliveredOrder = null;
      for (Row order: orders) {
        if (order.isNull("o_carrier_id")) {
          yetDeliveredOrder = order;
          break;
        }
      }
      if (yetDeliveredOrder == null) {
        System.out.printf("Cannot find any yet-to-be-delivered order in warehouse %d district %d.\n", warehouseID, i);
        break;
      }
      int orderID = yetDeliveredOrder.getInt("o_id");
      System.out.printf("The oldest yet-to-be-delivered order in warehouse %d district %d is %d.\n",
          warehouseID, i, orderID);

      // Updates the carrier.
      query = String.format(UPDATE_CARRIER, carrierID, warehouseID, i, orderID);
      executeQuery(query);

      // Updates the delivery date.
      query = String.format(GET_ORDER_LINE_FROM_ORDER, warehouseID, i, orderID);
      List<Row> orderLineNumbers = executeQuery(query);
      StringBuilder builder = new StringBuilder();
      for (int j = 0; j < orderLineNumbers.size(); j++) {
        int orderLineID = orderLineNumbers.get(j).getInt("ol_number");
        builder.append(orderLineID);
        if (j != orderLineNumbers.size() - 1) {
          builder.append(", ");
        }
      }
      query = String.format(UPDATE_DELIVERY_DATE, warehouseID, i, orderID, builder.toString());
      System.out.printf("Going to update delivery date with query %s.\n", query);
      executeQuery(query);

      // Gets the total amount.
      query = String.format(ORDER_LINE_TOTAL_AMOUNT, warehouseID, i, orderID);
      double totalAmount = executeQuery(query).get(0).getBigDecimal(0).doubleValue();
      System.out.printf("The total amount for this order is %f.\n", totalAmount);

      // Updates the customer.
      int customerID = yetDeliveredOrder.getInt("o_c_id");
      query = String.format(GET_CUSTOMER, warehouseID, i, customerID);
      Row customer = executeQuery(query).get(0);
      double newBalance = customer.getBigDecimal("c_balance").doubleValue() + totalAmount;
      int newDeliveryCount = customer.getInt("c_delivery_cnt") + 1;
      query = String.format(UPDATE_CUSTOMER, newBalance, newDeliveryCount, warehouseID, i, customerID);
      System.out.printf("Going to update customer by query %s.\n", query);
      executeQuery(query);
    }
  }
}
