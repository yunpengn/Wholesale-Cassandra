package edu.cs4224.transactions;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Row;

import edu.cs4224.ScalingParameters;

public class FinalStateTransaction extends BaseTransaction {
  private static final String QUERY_WAREHOUSE = "SELECT SUM(w_ytd) FROM warehouse";
  private static final String QUERY_DISTRICT = "SELECT SUM(d_ytd), SUM(d_next_o_id) FROM district_w";
  private static final String QUERY_CUSTOMER
      = "SELECT SUM(c_balance), SUM(c_ytd_payment), SUM(c_payment_cnt), SUM(c_delivery_cnt) from customer_w";

  public FinalStateTransaction(final CqlSession session, final String[] parameters) {
    super(session, parameters);
  }

  @Override public void execute(final String[] dataLines) {
    System.out.println("\n======================================================================");
    Row row = executeQuery(QUERY_WAREHOUSE).get(0);
    System.out.printf("Year-to-date total amount paid to warehouses: %d\n", row.getInt(0));

    row = executeQuery(QUERY_DISTRICT).get(0);
    double sum = ScalingParameters.fromDB(row.getLong(0), ScalingParameters.SCALE_D_YTD);
    System.out.printf("Year-to-date total amount paid to districts: %f\n", sum);
    sum = ScalingParameters.fromDB(row.getLong(1), ScalingParameters.SCALE_D_NEXT_O_ID);
    System.out.printf("Sum of next orderIDs in all districts: %f\n", sum);

    row = executeQuery(QUERY_CUSTOMER).get(0);
    sum = ScalingParameters.fromDB(row.getLong(0), ScalingParameters.SCALE_C_BALANCE);
    System.out.printf("Sum of balanced of all customers: %f\n", sum);

    System.out.println("\n======================================================================");
  }
}
