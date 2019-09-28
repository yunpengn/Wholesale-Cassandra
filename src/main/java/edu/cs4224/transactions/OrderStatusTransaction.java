package edu.cs4224.transactions;

import com.datastax.oss.driver.api.core.CqlSession;

public class OrderStatusTransaction extends BaseTransaction {
  public OrderStatusTransaction(final CqlSession session, final String[] parameters) {
    super(session, parameters);
  }

  @Override public void execute(final String[] dataLines) {

  }
}
