package edu.cs4224.transactions;

import com.datastax.oss.driver.api.core.CqlSession;

public class PopularItemTransaction extends BaseTransaction {
  public PopularItemTransaction(final CqlSession session, final String[] parameters) {
    super(session, parameters);
  }

  @Override public void execute(final String[] dataLines) {

  }
}
