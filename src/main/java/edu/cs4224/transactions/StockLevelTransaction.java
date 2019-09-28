package edu.cs4224.transactions;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Row;

import java.util.List;

public class StockLevelTransaction extends BaseTransaction {
  private static final String GET_DISTRICT
      = "SELECT * FROM district WHERE d_w_id = %d AND d_id = %d";
  private static final String LAST_L_ORDERS
      = "SELECT * FROM order_line WHERE ol_w_id = %d AND ol_d_id = %d AND ol_o_id >= %d AND ol_o_id < %d";
  private static final String STOCK_BELOW_THRESHOLD
      = "SELECT * FROM stock WHERE s_w_id = %d AND s_i_id = %d";

  private final int warehouseID;
  private final int districtID;
  private final int threshold;
  private final int numOrders;

  public StockLevelTransaction(final CqlSession session, final String[] parameters) {
    super(session, parameters);

    warehouseID = Integer.parseInt(parameters[1]);
    districtID = Integer.parseInt(parameters[2]);
    threshold = Integer.parseInt(parameters[3]);
    numOrders = Integer.parseInt(parameters[4]);
  }

  @Override public void execute(final String[] dataLines) {
    String query = String.format(GET_DISTRICT, warehouseID, districtID);
    Row district = executeQuery(query).get(0);
    int nextAvailableOrderID = district.getInt("d_next_o_id");

    query = String.format(LAST_L_ORDERS, warehouseID, districtID,
        nextAvailableOrderID - numOrders, nextAvailableOrderID);
    List<Row> orderLine = executeQuery(query);

    int count = 0;
    for (Row orderItem: orderLine) {
      int itemID = orderItem.getInt("ol_i_id");
      query = String.format(STOCK_BELOW_THRESHOLD, warehouseID, itemID);
      Row stock = executeQuery(query).get(0);
      if (stock.getBigDecimal("s_quantity").doubleValue() < threshold) {
        count++;
      }
    }

    System.out.printf("Number of items below threshold: %d\n", count);
  }
}
