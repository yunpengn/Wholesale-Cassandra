package edu.cs4224.transactions;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.SimpleStatementBuilder;

import java.time.Duration;
import java.util.List;

/**
 * BaseTransaction is base class for all different transactions.
 */
public abstract class BaseTransaction {
  private final String[] parameters;
  protected final CqlSession session;
  private String consistencyLevel = "ALL_ONE";

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

  /**
   * Executes a given query string.
   *
   * @param query is the given query string.
   * @return all rows in the result set.
   */
  protected List<Row> executeQuery(String query) {
    SimpleStatement statement = new SimpleStatementBuilder(query)
        .setConsistencyLevel(getConsistencyLevel(query))
        .build();
    ResultSet resultSet = session.execute(statement);
    return resultSet.all();
  }

  /**
   * Executes a given query string.
   *
   * @param query is the given query string.
   * @param timeoutInSeconds is the length of timeout in seconds.
   * @return all rows in the result set.
   */
  protected List<Row> executeQuery(String query, int timeoutInSeconds) {
    SimpleStatement statement = new SimpleStatementBuilder(query)
        .setConsistencyLevel(getConsistencyLevel(query))
        .setTimeout(Duration.ofSeconds(timeoutInSeconds))
        .build();
    ResultSet resultSet = session.execute(statement);
    return resultSet.all();
  }

  private ConsistencyLevel getConsistencyLevel(String query) {
    if (consistencyLevel.equals("ALL_ONE")) {
      if (query.startsWith("SELECT")) {
        return ConsistencyLevel.ONE;
      } else {
        return ConsistencyLevel.ALL;
      }
    } else {
      return ConsistencyLevel.QUORUM;
    }
  }

  public void setConsistencyLevel(String consistencyLevel) {
    this.consistencyLevel = consistencyLevel;
  }
}
