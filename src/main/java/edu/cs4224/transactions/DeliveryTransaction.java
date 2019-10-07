package edu.cs4224.transactions;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Row;

import edu.cs4224.OrderlineInfo;
import edu.cs4224.OrderlineInfoMap;

import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

public class DeliveryTransaction extends BaseTransaction {
  private static final String YET_DELIVERED_ORDER
      = "SELECT d_next_delivery_o_id FROM district_w WHERE d_w_id = %d AND d_id = %d";
  private static final String UPDATE_YET_DELIVERED_ORDER
      = "UPDATE district_w SET d_next_delivery_o_id = d_next_delivery_o_id + 1 WHERE d_w_id = %d AND d_id = %d";
  private static final String GET_ORDER
      = "SELECT o_l_info, o_c_id FROM customer_order WHERE o_w_id = %d AND o_d_id = %d AND o_id = %d";
  private static final String UPDATE_CARRIER_DELIVERY
      = "UPDATE customer_order SET o_carrier_id = %d, o_l_info = '%s' WHERE o_w_id = %d AND o_d_id = %d AND o_id = %d";
  private static final String GET_CUSTOMER
      = "SELECT c_balance, c_delivery_cnt FROM customer_w WHERE c_w_id = %d AND c_d_id = %d AND c_id = %d";
  private static final String UPDATE_CUSTOMER
      = "UPDATE customer_w SET c_balance = %f, c_delivery_cnt = %d "
      + "WHERE c_w_id = %d AND c_d_id = %d AND c_id = %d";
  private static final int NUM_DISTRICTS = 10;
  private static final Format formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

  private final int warehouseID;
  private final int carrierID;

  public DeliveryTransaction(final CqlSession session, final String[] parameters) {
    super(session, parameters);

    warehouseID = Integer.parseInt(parameters[1]);
    carrierID = Integer.parseInt(parameters[2]);
  }

  @Override public void execute(final String[] dataLines) {
    for (int i = 1; i <= NUM_DISTRICTS; i++) {
      // Finds the ID of the oldest yet-to-be-delivered order.
      String query = String.format(YET_DELIVERED_ORDER, warehouseID, i);
      long orderID = executeQuery(query).get(0).getLong("d_next_delivery_o_id");
      System.out.printf("The oldest yet-to-be-delivered order in warehouse %d district %d is %d.\n",
          warehouseID, i, orderID);

      // Updates the ID of the oldest yet-to-be-delivered order.
      query = String.format(UPDATE_YET_DELIVERED_ORDER, warehouseID, i);
      executeQuery(query);

      // Finds the corresponding order.
      query = String.format(GET_ORDER, warehouseID, i, orderID);
      List<Row> orders = executeQuery(query);
      if (orders.isEmpty()) {
        System.out.printf("Unable to find order in warehouse=%d, district=%d, orderID=%d.\n", warehouseID, i, orderID);
        return;
      }
      Row yetDeliveredOrder = orders.get(0);
      OrderlineInfoMap orderLines = OrderlineInfoMap.fromJson(yetDeliveredOrder.getString("o_l_info"));

      // Updates the carrier and delivery date.
      double totalAmount = 0;
      for (OrderlineInfo orderLine: orderLines.values()) {
        totalAmount += orderLine.getAmount();
        orderLine.setDelivery(formatter.format(new Date()));
      }
      query = String.format(UPDATE_CARRIER_DELIVERY, carrierID, orderLines.toJson(), warehouseID, i, orderID);
      System.out.printf("Going to update order by query %s.\n", query);
      executeQuery(query);

      // Finds the customer.
      int customerID = yetDeliveredOrder.getInt("o_c_id");
      query = String.format(GET_CUSTOMER, warehouseID, i, customerID);
      Row customer = executeQuery(query).get(0);
      double newAmount = customer.getBigDecimal("c_balance").doubleValue() + totalAmount;
      int newDeliveryCount = customer.getInt("c_delivery_cnt") + 1;

      // Updates the customer.
      query = String.format(UPDATE_CUSTOMER, newAmount, newDeliveryCount, warehouseID, i, customerID);
      System.out.printf("Going to update customer by query %s.\n", query);
      executeQuery(query);
    }
  }
}
