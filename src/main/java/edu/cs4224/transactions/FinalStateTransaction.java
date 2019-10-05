package edu.cs4224.transactions;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.SimpleStatementBuilder;

import edu.cs4224.ScalingParameters;

import java.time.Duration;

public class FinalStateTransaction extends BaseTransaction {
  private static final String QUERY_WAREHOUSE = "SELECT SUM(w_ytd) FROM warehouse";
  private static final String QUERY_DISTRICT = "SELECT SUM(d_ytd), SUM(d_next_o_id) FROM district_w";
  private static final String QUERY_CUSTOMER
      = "SELECT SUM(c_balance), SUM(c_ytd_payment), SUM(c_payment_cnt), SUM(c_delivery_cnt) from customer_w";
  private static final String QUERY_ORDER = "SELECT MAX(o_id), SUM(o_ol_cnt) from customer_order";
  private static final String QUERY_STOCK
      = "SELECT SUM(s_quantity), SUM(s_ytd), SUM(s_order_cnt), SUM(s_remote_cnt) from stock_w";

  public FinalStateTransaction(final CqlSession session, final String[] parameters) {
    super(session, parameters);
  }

  @Override public void execute(final String[] dataLines) {
    System.out.println("\n======================================================================");
    Row row = executeQuery(QUERY_WAREHOUSE).get(0);
    System.out.printf("Year-to-date total amount paid to warehouses: %f\n", row.getBigDecimal(0).doubleValue());

    row = executeQuery(QUERY_DISTRICT).get(0);
    double sum = ScalingParameters.fromDB(row.getLong(0), ScalingParameters.SCALE_D_YTD);
    System.out.printf("Year-to-date total amount paid to districts: %f\n", sum);
    sum = ScalingParameters.fromDB(row.getLong(1), ScalingParameters.SCALE_D_NEXT_O_ID);
    System.out.printf("Sum of next orderIDs in all districts: %f\n", sum);

    row = executeQuery(QUERY_CUSTOMER).get(0);
    sum = row.getBigDecimal(0).doubleValue();
    System.out.printf("Sum of balance of all customers: %f\n", sum);
    sum = row.getFloat(1);
    System.out.printf("Sum of year-to-date payment of all customers: %f\n", sum);
    int paymentCounterSum = row.getInt(2);
    System.out.printf("Sum of payment counter of all customers: %d\n", paymentCounterSum);
    int deliveryCounterSum = row.getInt(3);
    System.out.printf("Sum of delivery counter of all customers: %d\n", deliveryCounterSum);

    row = executeQuery(QUERY_ORDER).get(0);
    int orderIdSum = row.getInt(0);
    System.out.printf("Sum of orderIDs of all orders: %d\n", orderIdSum);
    double orderLineCountSum = row.getBigDecimal(1).doubleValue();
    System.out.printf("Sum of orderLine count of all orders: %f\n", orderLineCountSum);

    SimpleStatement statement = new SimpleStatementBuilder(QUERY_STOCK)
        .setTimeout(Duration.ofHours(1))
        .build();
    row = session.execute(statement).one();
    sum = ScalingParameters.fromDB(row.getLong(0), ScalingParameters.SCALE_S_QUANTITY);
    System.out.printf("Sum of quantity of all stocks: %f\n", sum);
    sum = ScalingParameters.fromDB(row.getLong(1), ScalingParameters.SCALE_S_YTD);
    System.out.printf("Sum of year-to-date payment of all stocks: %f\n", sum);
    sum = ScalingParameters.fromDB(row.getLong(2), ScalingParameters.SCALE_S_ORDER_CNT);
    System.out.printf("Sum of order count of all stocks: %f\n", sum);
    sum = ScalingParameters.fromDB(row.getLong(3), ScalingParameters.SCALE_S_REMOTE_CNT);
    System.out.printf("Sum of remote order count of all stocks: %f\n", sum);

    System.out.println("\n======================================================================");
  }
}
