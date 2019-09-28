package edu.cs4224.transactions;

/**
 * NewOrderTransaction is the transaction used to create a new order.
 */
public class NewOrderTransaction extends BaseTransaction {
  private final int numDataLines;

  public NewOrderTransaction(final String[] parameters) {
    super(parameters);

    String lastParam = parameters[parameters.length - 1];
    numDataLines = Integer.parseInt(lastParam);
  }

  @Override public int numOfDataLines() {
    return numDataLines;
  }

  @Override public void execute(final String[] dataLines) {

  }
}
