package edu.cs4224.transactions;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Row;

import edu.cs4224.OrderlineInfo;
import edu.cs4224.OrderlineInfoMap;
import edu.cs4224.ScalingParameters;

public class OrderStatusTransaction extends BaseTransaction {
  private static final String GET_CUSTOMER_R
      = "SELECT c_first, c_middle, c_last FROM customer_r WHERE c_w_id = %d AND c_d_id = %d AND c_id = %d";
  private static final String GET_CUSTOMER_BALANCE
      = "SELECT c_balance FROM customer_w WHERE c_w_id = %d AND c_d_id = %d AND c_id = %d";
  private static final String CUSTOMER_LAST_ORDER
      = "SELECT o_id, o_entry_d, o_carrier_id, o_l_info FROM customer_order "
      + "WHERE o_w_id = %d AND o_d_id = %d AND o_c_id = %d "
      + "ORDER BY o_id DESC LIMIT 1 ALLOW FILTERING";

  private final int warehouseID;
  private final int districtID;
  private final int customerID;

  public OrderStatusTransaction(final CqlSession session, final String[] parameters) {
    super(session, parameters);

    warehouseID = Integer.parseInt(parameters[1]);
    districtID = Integer.parseInt(parameters[2]);
    customerID = Integer.parseInt(parameters[3]);
  }

  @Override public void execute(final String[] dataLines) {
    // Gets the customer's information.
    String query = String.format(GET_CUSTOMER_R, warehouseID, districtID, customerID);
    Row customerR = executeQuery(query).get(0);
    query = String.format(GET_CUSTOMER_BALANCE, warehouseID, districtID, customerID);
    Row customerW = executeQuery(query).get(0);
    System.out.printf("Customer name: %s %s %s, balance: %f\n",
        customerR.getString("c_first"),
        customerR.getString("c_middle"),
        customerR.getString("c_last"),
        customerW.getBigDecimal("c_balance").doubleValue());

    // Gets the customer's last order information.
    query = String.format(CUSTOMER_LAST_ORDER, warehouseID, districtID, customerID);
    Row lastOrder = executeQuery(query).get(0);
    int lastOrderID = lastOrder.getInt("o_id");
    System.out.printf("Customer's last order ID: %d, entry time: %s, carrier ID: %d\n",
        lastOrderID,
        lastOrder.getInstant("o_entry_d").toString(),
        lastOrder.getInt("o_carrier_id"));

    // Retrieves each orderLine from JSON content.
    OrderlineInfoMap orderLines = OrderlineInfoMap.fromJson(lastOrder.getString("o_l_info"));
    for (OrderlineInfo orderLine: orderLines.values()) {
      System.out.printf("Order line in last order item ID: %d, supply warehouse ID: %d, "
              + "quantity: %f, price: %f, delivery date: %s\n",
          orderLine.getId(),
          orderLine.getSupply(),
          orderLine.getQuantity(),
          orderLine.getAmount(),
          orderLine.getDelivery());
    }
  }
}
