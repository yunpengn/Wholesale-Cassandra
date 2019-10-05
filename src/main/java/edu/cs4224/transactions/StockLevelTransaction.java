package edu.cs4224.transactions;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Row;

import edu.cs4224.OrderlineInfo;
import edu.cs4224.OrderlineInfoMap;
import edu.cs4224.ScalingParameters;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class StockLevelTransaction extends BaseTransaction {
  private static final String GET_DISTRICT
      = "SELECT d_next_o_id FROM district_w WHERE d_w_id = %d AND d_id = %d";
  private static final String LAST_L_ORDERS
      = "SELECT o_l_info FROM customer_order WHERE o_w_id = %d AND o_d_id = %d AND o_id >= %d AND o_id < %d";
  private static final String STOCK_BELOW_THRESHOLD
      = "SELECT s_quantity FROM stock_w WHERE s_w_id = %d AND s_i_id IN (%s)";

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
    // Gets next available orderID.
    String query = String.format(GET_DISTRICT, warehouseID, districtID);
    Row district = executeQuery(query).get(0);
    int nextAvailableOrderID = district.getInt("d_next_o_id");

    // Gets the last L orders.
    query = String.format(LAST_L_ORDERS, warehouseID, districtID,
        nextAvailableOrderID - numOrders, nextAvailableOrderID);
    List<Row> orders = executeQuery(query);

    // Gets the itemIDs in the last L orders.
    Set<Integer> itemIDs = new HashSet<>();
    for (Row order: orders) {
      OrderlineInfoMap orderLines = OrderlineInfoMap.fromJson(order.getString("o_l_info"));
      for (OrderlineInfo orderLine: orderLines.values()) {
        itemIDs.add(orderLine.getId());
      }
    }

    // Gets the number of items below threshold.
    StringBuilder builder = new StringBuilder();
    for (int itemID: itemIDs) {
      builder.append(itemID);
      builder.append(", ");
    }
    String set = builder.length() > 0 ? builder.substring(0, builder.length() - 2) : builder.toString();
    int count = 0;
    query = String.format(STOCK_BELOW_THRESHOLD, warehouseID, set);
    for (Row stock: executeQuery(query)) {
      if (ScalingParameters.fromDB(stock.getLong("s_quantity"), ScalingParameters.SCALE_S_QUANTITY) < threshold) {
        count++;
      }
    }

    System.out.printf("Number of items below threshold: %d\n", count);
  }
}
