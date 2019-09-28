package edu.cs4224.transactions;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Row;
import edu.cs4224.CqlQueryList;

public class PaymentTransaction extends BaseTransaction {

  private final int customer_warehouse_id;
  private final int customer_district_id;
  private final int customer_id;
  private final int payment_amount;

  public PaymentTransaction(final CqlSession session, final String[] parameters) {
      super(session, parameters);
      customer_warehouse_id = Integer.parseInt(parameters[1]);
      customer_district_id = Integer.parseInt(parameters[2]);
      customer_id = Integer.parseInt(parameters[3]);
      payment_amount = Integer.parseInt(parameters[4]);
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


  }
}
