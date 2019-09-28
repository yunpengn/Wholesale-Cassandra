package edu.cs4224.transactions;

import com.datastax.oss.driver.api.core.CqlSession;

public class RelatedCustomerTransaction extends BaseTransaction {
  public RelatedCustomerTransaction(final CqlSession session, final String[] parameters) {
    super(session, parameters);
  }

  @Override public void execute(final String[] dataLines) {

  }
}
