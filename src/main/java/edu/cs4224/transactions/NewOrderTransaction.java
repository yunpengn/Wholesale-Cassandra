package edu.cs4224.transactions;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Row;

import edu.cs4224.CqlQueryList;

import java.util.ArrayList;
import java.util.List;

/**
 * NewOrderTransaction is the transaction used to create a new order.
 */
public class NewOrderTransaction extends BaseTransaction {
  private final int customerID;
  private final int warehouseID;
  private final int districtID;
  private final int numDataLines;

  public NewOrderTransaction(final CqlSession session, final String[] parameters) {
    super(session, parameters);

    customerID = Integer.parseInt(parameters[1]);
    warehouseID = Integer.parseInt(parameters[2]);
    districtID = Integer.parseInt(parameters[3]);
    numDataLines = Integer.parseInt(parameters[4]);
  }

  @Override public int numOfDataLines() {
    return numDataLines;
  }

  @Override public void execute(final String[] dataLines) {
    List<Integer> numsEach = new ArrayList<>();
    List<Integer> supplierWarehouse = new ArrayList<>();
    List<Integer> quantity = new ArrayList<>();

    for (String dataLine: dataLines) {
      String[] parts = dataLine.split(",");
      numsEach.add(Integer.parseInt(parts[0]));
      supplierWarehouse.add(Integer.parseInt(parts[1]));
      quantity.add(Integer.parseInt(parts[2]));
    }

    createNewOrder(numsEach, supplierWarehouse, quantity);
  }

  private void createNewOrder(List<Integer> numsEach, List<Integer> supplierWareHouse, List<Integer> quantity) {
    String query = String.format(CqlQueryList.DISTRICT_NEXT_ORDER_ID, warehouseID, districtID);
    List<Row> rows = executeQuery(query);
    System.out.println(rows.get(0));
  }
}
