package edu.cs4224.transactions;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Row;

import edu.cs4224.CqlQueryList;

import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * NewOrderTransaction is the transaction used to create a new order.
 */
public class NewOrderTransaction extends BaseTransaction {
  private final int customerID;
  private final int warehouseID;
  private final int districtID;
  private final int numDataLines;
  private static final Format formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS z");


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
    List<Integer> itemIds = new ArrayList<>();
    List<Integer> supplierWarehouse = new ArrayList<>();
    List<Integer> quantity = new ArrayList<>();

    for (String dataLine: dataLines) {
      String[] parts = dataLine.split(",");
      itemIds.add(Integer.parseInt(parts[0]));
      supplierWarehouse.add(Integer.parseInt(parts[1]));
      quantity.add(Integer.parseInt(parts[2]));
    }

    createNewOrder(itemIds, supplierWarehouse, quantity);
  }

  private void createNewOrder(List<Integer> itemIds, List<Integer> supplierWareHouse, List<Integer> quantity) {
    String get_next_order_number_query = String.format(CqlQueryList.DISTRICT_INFO, warehouseID, districtID);
    Row res = executeQuery(get_next_order_number_query).get(0);
    int next_order_number = res.getInt("D_NEXT_O_ID");
    double district_tax = res.getBigDecimal("D_TAX").doubleValue();
    String increment_order_number_query =  String.format(CqlQueryList.INCREMENT_NEXT_ORDER_ID, warehouseID, districtID);
    executeQuery(increment_order_number_query);

    int isAllLocal = 1;
    for (Integer integer : supplierWareHouse) {
      if (integer != warehouseID) {
        isAllLocal = 0;
        break;
      }
    }

    Date cur = new Date();
    String order_time = formatter.format(cur);

    String new_order_query = String.format(CqlQueryList.CREATE_NEW_ORDER, next_order_number, districtID,
            warehouseID, customerID, order_time, numDataLines, isAllLocal);
    executeQuery(new_order_query);

    ArrayList<Integer> adjustedQuantities = new ArrayList<>();
    ArrayList<Double> itemsAmount = new ArrayList<>();
    ArrayList<String> itemsName = new ArrayList<>();

    double totalAmount = 0;
    for (int i = 0; i < numDataLines; i++) {
        String check_stock_quantity_query = String.format(CqlQueryList.CHECK_STOCK_QUANTITY,
                supplierWareHouse.get(i), itemIds.get(i));
        int quantity_left = executeQuery(check_stock_quantity_query).get(0).getBigDecimal("S_QUANTITY").intValue();
        int adjusted_quantity = quantity_left - quantity.get(i);
        if (adjusted_quantity < 10) adjusted_quantity += 100;
        adjustedQuantities.add(adjusted_quantity);
        int remoteIncrement =  supplierWareHouse.get(i) == warehouseID ? 0 : 1;
        String update_stock_query = String.format(CqlQueryList.UPDATE_STOCK, adjusted_quantity, quantity.get(i),
                remoteIncrement, supplierWareHouse.get(i), itemIds.get(i));
        executeQuery(update_stock_query);

        String check_item_price_query = String.format(CqlQueryList.CHECK_ITEM_INFO, itemIds.get(i));
        Row item_info = executeQuery(check_item_price_query).get(0);
        double item_price = item_info.getDouble("I_PRICE");
        String item_name = item_info.getString("I_NAME");
        double itemAmount = quantity.get(i) * item_price;
        itemsAmount.add(itemAmount);
        itemsName.add(item_name);
        totalAmount += itemAmount;
        String create_order_line_query = String.format(CqlQueryList.CREATE_ORDER_LINE, next_order_number,
                districtID, warehouseID, i + 1, itemIds.get(i), supplierWareHouse.get(i), quantity.get(i),itemAmount, "S_DIST_" + districtID);
        executeQuery(create_order_line_query);
    }
    String check_warehouse_tax_query = String.format(CqlQueryList.CHECK_WAREHOUSE_TAX, warehouseID);
    double warehouse_tax = executeQuery(check_warehouse_tax_query).get(0).getDouble("W_TAX");

    String check_customer_discount_query = String.format(CqlQueryList.CHECK_CUSTOMER_INFO, warehouseID, districtID, customerID);
    Row customer_info = executeQuery(check_customer_discount_query).get(0);
    double user_discount = customer_info.getDouble("C_DISCOUNT");
    String user_last_name = customer_info.getString("C_LAST");
    String user_credit_status = customer_info.getString("C_CREDIT");


    totalAmount = totalAmount * (1 + warehouse_tax + district_tax) * (1 - user_discount);

    System.out.println("Transaction Summary:");
    System.out.println(String.format("1. (W_ID: %d, D_ID: %d, C_ID, %d), C_LAST: %s, C_CREDIT: %s, C_DISCOUNT: %.4f",
            warehouseID, districtID, customerID, user_last_name, user_credit_status, user_discount));
    System.out.println(String.format("2. W_TAX: %.4f, D_TAX: %.4f", warehouse_tax, district_tax));
    System.out.println(String.format("3. O_ID: %d, O_ENTRY_D: %s", next_order_number,  order_time));
    System.out.println(String.format("4. NUM_ITEMS: %s, TOTAL_AMOUNT: %.2f", numDataLines, totalAmount));
    for (int i = 0; i < numDataLines; i++) {
      System.out.println(String.format("\t ITEM_NUMBER: %s, I_NAME: %s, SUPPLIER_WAREHOUSE: %d, QUANTITY: %d, OL_AMOUNT: %.2f, S_QUANTITY: %d",
              itemIds.get(i), itemsName.get(i), supplierWareHouse.get(i), quantity.get(i), itemsAmount.get(i), adjustedQuantities.get(i)));
    }
  }
}
