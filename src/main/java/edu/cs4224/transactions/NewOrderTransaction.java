package edu.cs4224.transactions;

/**
 * NewOrderTransaction is the transaction used to create a new order.
 */
public class NewOrderTransaction extends BaseTransaction {
  private final int customerID;
  private final int warehouseID;
  private final int districtID;
  private final int numDataLines;

  public NewOrderTransaction(final String[] parameters) {
    super(parameters);

    customerID = Integer.parseInt(parameters[1]);
    warehouseID = Integer.parseInt(parameters[2]);
    districtID = Integer.parseInt(parameters[3]);
    numDataLines = Integer.parseInt(parameters[4]);
  }

  @Override public int numOfDataLines() {
    return numDataLines;
  }

  @Override public void execute(final String[] dataLines) {

  }
}
