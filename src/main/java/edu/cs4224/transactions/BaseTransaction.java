package edu.cs4224.transactions;

/**
 * BaseTransaction is base class for all different transactions.
 */
public abstract class BaseTransaction {
  private final String[] parameters;

  public BaseTransaction(final String[] parameters) {
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
}
