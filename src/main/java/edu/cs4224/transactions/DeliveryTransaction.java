package edu.cs4224.transactions;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Row;

import edu.cs4224.OrderlineInfo;
import edu.cs4224.OrderlineInfoMap;
import edu.cs4224.ScalingParameters;

import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.Date;

public class DeliveryTransaction extends BaseTransaction {
  private static final String YET_DELIVERED_ORDER
      = "SELECT d_next_delivery_o_id FROM district_w WHERE d_w_id = %d AND d_id = %d";
  private static final String UPDATE_YET_DELIVERED_ORDER
      = "UPDATE district_w SET d_next_delivery_o_id = d_next_delivery_o_id + 1 WHERE d_w_id = %d AND d_id = %d";
  private static final String GET_ORDER
      = "SELECT o_l_info FROM customer_order WHERE o_w_id = %d AND o_d_id = %d AND o_id = %d";
  private static final String UPDATE_CARRIER_DELIVERY
      = "UPDATE customer_order SET o_carrier_id = %d, o_l_info = %s WHERE o_w_id = %d AND o_d_id = %d AND o_id = %d";
  private static final String UPDATE_CUSTOMER
      = "UPDATE customer_w SET c_balance = c_balance + %s, c_delivery_cnt = c_delivery_cnt + 1 "
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
      int orderID = executeQuery(query).get(0).getInt("d_next_delivery_o_id");
      System.out.printf("The oldest yet-to-be-delivered order in warehouse %d district %d is %d.\n",
          warehouseID, i, orderID);

      // Updates the ID of the oldest yet-to-be-delivered order.
      query = String.format(UPDATE_YET_DELIVERED_ORDER, warehouseID, 1);
      executeQuery(query);

      // Finds the corresponding order.
      query = String.format(GET_ORDER, warehouseID, i, orderID);
      Row yetDeliveredOrder = executeQuery(query).get(0);
      OrderlineInfoMap orderLines = OrderlineInfoMap.fromJson(yetDeliveredOrder.getString("o_l_info"));
      System.out.println(yetDeliveredOrder.getString("o_l_info"));

      // Updates the carrier and delivery date.
      double totalAmount = 0;
      for (OrderlineInfo orderLine: orderLines.values()) {
        totalAmount += orderLine.getAMOUNT();
        orderLine.setDELIVERY_D(formatter.format(new Date()));
      }
      query = String.format(UPDATE_CARRIER_DELIVERY, carrierID, orderLines.toJson(), warehouseID, i, orderID);
      System.out.printf("Going to update order by query %s.\n", query);
      executeQuery(query);

      // Updates the customer.
      int customerID = yetDeliveredOrder.getInt("o_c_id");
      String amountDiff = ScalingParameters.toDB(totalAmount, ScalingParameters.SCALE_C_BALANCE);
      query = String.format(UPDATE_CUSTOMER, amountDiff, warehouseID, i, customerID);
      System.out.printf("Going to update customer by query %s.\n", query);
      executeQuery(query);
    }
  }
}
