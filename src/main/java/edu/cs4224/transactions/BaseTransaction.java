package edu.cs4224.transactions;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.SimpleStatementBuilder;

import java.util.List;

/**
 * BaseTransaction is base class for all different transactions.
 */
public abstract class BaseTransaction {
  private final String[] parameters;
  protected final CqlSession session;

  public BaseTransaction(final CqlSession session, final String[] parameters) {
    this.session = session;
    this.parameters = parameters;
  }

  /**
   * The number of lines that should be followed after the parameter line.
   *
   * @return the number of lines of inputs to be served as data.
   */
  public int numOfDataLines() {
    return 0;
  }

  /**
   * Reads the input data lines and executes the transaction.
   *
   * @param dataLines are the lines of input data.
   */
  public abstract void execute(String[] dataLines);

  protected List<Row> executeQuery(String query) {
    SimpleStatement statement = new SimpleStatementBuilder(query)
        .setConsistencyLevel(ConsistencyLevel.EACH_QUORUM)
        .build();
    ResultSet resultSet = session.execute(statement);
    return resultSet.all();
  }
}
