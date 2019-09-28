package edu.cs4224.transactions;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Row;
import edu.cs4224.CqlQueryList;

public class PaymentTransaction extends BaseTransaction {

  private final int customer_warehouse_id;
  private final int customer_district_id;
  private final int customer_id;
  private final double payment_amount;

  public PaymentTransaction(final CqlSession session, final String[] parameters) {
      super(session, parameters);
      customer_warehouse_id = Integer.parseInt(parameters[1]);
      customer_district_id = Integer.parseInt(parameters[2]);
      customer_id = Integer.parseInt(parameters[3]);
      payment_amount = Double.parseDouble(parameters[4]);
  }

  @Override public void execute(final String[] dataLines) {
    Row warehouse_info = executeQuery(String.format(CqlQueryList.GET_WAREHOUSE_INFO, customer_warehouse_id)).get(0);
    double warehouse_ytd = warehouse_info.getBigDecimal("W_YTD").doubleValue();
    executeQuery(String.format(CqlQueryList.UPDATE_WAREHOUSE_YTD, warehouse_ytd + payment_amount, customer_warehouse_id));

    Row district_info = executeQuery(String.format(CqlQueryList.GET_DISTRICT_INFO, customer_warehouse_id,
            customer_district_id)).get(0);
    double district_ytd = district_info.getBigDecimal("D_YTD").doubleValue();
    executeQuery(String.format(CqlQueryList.UPDATE_DISTRICT_YTD, district_ytd + payment_amount, customer_warehouse_id,
            customer_district_id));

    Row customer_info = executeQuery(String.format(CqlQueryList.GET_CUSTOMER_INFO, customer_warehouse_id,
            customer_district_id, customer_id)).get(0);
    double customer_balance = customer_info.getBigDecimal("C_BALANCE").doubleValue();
    float customer_ytd_payment = customer_info.getFloat("C_YTD_PAYMENT");
    int customer_payment_cnt = customer_info.getInt("C_PAYMENT_CNT");

    executeQuery(String.format(CqlQueryList.UPDATE_CUSTOMER_INFO, customer_balance, customer_ytd_payment,
            customer_payment_cnt, customer_warehouse_id, customer_district_id, customer_id));

    System.out.println("Transaction Summary: ");
    System.out.println(String.format("1. (C_W_ID: %d, C_D_ID: %d, C_ID: %d), Name: (%s, %s, %s), Address: (%s, %s, %s, %s, %s), C_PHONE: %s, C_SINCE: %s, C_CREDIT: %s, C_CREDIT_LIM: %.2f, C_DISCOUNT: %.4f, C_BALANCE: %.2f",
            customer_warehouse_id, customer_district_id, customer_id, customer_info.getString("C_FIRST"), customer_info.getString("C_MIDDLE"), customer_info.getString("C_LAST"),
            customer_info.getString("C_STREET_1"), customer_info.getString("C_STREET_2"), customer_info.getString("C_CITY"),
            customer_info.getString("C_STATE"), customer_info.getString("C_ZIP"), customer_info.getString("C_PHONE"),
            customer_info.getLocalTime("C_SINCE").toString(), customer_info.getString("C_CREDIT"), customer_info.getBigDecimal("C_CREDIT_LIM").doubleValue(),
            customer_info.getBigDecimal("C_DISCOUNT").doubleValue(), customer_info.getBigDecimal("C_BALANCE").doubleValue()
    ));

    System.out.println(String.format("2. Warehouse: %s, %s, %s, %s, %s",
            warehouse_info.getString("W_STREET_1"), warehouse_info.getString("W_STREET_2"), warehouse_info.getString("W_CITY"), warehouse_info.getString("W_STATE"), warehouse_info.getString("W_ZIP")));

    System.out.println(String.format("3. District: %s, %s, %s, %s, %s",
          district_info.getString("D_STREET_1"), district_info.getString("D_STREET_2"), district_info.getString("D_CITY"), district_info.getString("D_STATE"), district_info.getString("D_ZIP")));
    System.out.println(String.format("4. PAYMENT: %.2f", payment_amount));
  }
}
