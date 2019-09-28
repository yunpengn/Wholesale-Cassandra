package edu.cs4224.transactions;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Row;

import java.util.List;

public class OrderStatusTransaction extends BaseTransaction {
  private static final String GET_CUSTOMER
      = "SELECT * FROM customer WHERE c_w_id = %d AND c_d_id = %d AND c_id = %d";
  private static final String CUSTOMER_LAST_ORDER
      = "SELECT * FROM customer_order WHERE o_w_id = %d AND o_d_id = %d AND o_c_id = %d "
      + "ORDER BY o_d_id DESC, o_id DESC LIMIT 1 ALLOW FILTERING";
  private static final String CUSTOMER_LAST_ORDER_LINE
      = "SELECT * FROM order_line WHERE ol_w_id = %d AND ol_d_id = %d AND ol_o_id = %d";

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
    String query = String.format(GET_CUSTOMER, warehouseID, districtID, customerID);
    Row customer = executeQuery(query).get(0);
    System.out.printf("Customer name: %s %s %s, balance: %f\n",
        customer.getString("c_first"),
        customer.getString("c_middle"),
        customer.getString("c_last"),
        customer.getBigDecimal("c_balance").doubleValue());

    query = String.format(CUSTOMER_LAST_ORDER, warehouseID, districtID, customerID);
    Row lastOrder = executeQuery(query).get(0);
    int lastOrderID = lastOrder.getInt("o_id");
    System.out.printf("Customer's last order ID: %d, entry time: %s, carrier ID: %d\n",
        lastOrderID,
        lastOrder.getString("o_entry_d"),
        lastOrder.getInt("o_carrier_id"));

    query = String.format(CUSTOMER_LAST_ORDER_LINE, warehouseID, districtID, lastOrderID);
    List<Row> orderLines = executeQuery(query);
    for (Row orderLine: orderLines) {
      System.out.printf("Order line in last order item ID: %d, supply warehouse ID: %d, "
              + "quantity: %f, price: %f, delivery date: %s\n",
          orderLine.getInt("ol_i_id"),
          orderLine.getInt("ol_supply_w_id"),
          orderLine.getBigDecimal("ol_quantity").doubleValue(),
          orderLine.getBigDecimal("ol_amount").doubleValue(),
          orderLine.getString("ol_delivery_d"));
    }
  }
}
