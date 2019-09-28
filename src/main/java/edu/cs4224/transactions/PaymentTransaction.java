package edu.cs4224.transactions;

import com.datastax.oss.driver.api.core.CqlSession;

public class PaymentTransaction extends BaseTransaction {
  public PaymentTransaction(final CqlSession session, final String[] parameters) {
    super(session, parameters);
  }

  @Override public void execute(final String[] dataLines) {

  }
}
