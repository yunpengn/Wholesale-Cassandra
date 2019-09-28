package edu.cs4224;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;

import edu.cs4224.transactions.BaseTransaction;
import edu.cs4224.transactions.DeliveryTransaction;
import edu.cs4224.transactions.NewOrderTransaction;
import edu.cs4224.transactions.OrderStatusTransaction;
import edu.cs4224.transactions.PaymentTransaction;
import edu.cs4224.transactions.PopularItemTransaction;
import edu.cs4224.transactions.RelatedCustomerTransaction;
import edu.cs4224.transactions.StockLevelTransaction;
import edu.cs4224.transactions.TopBalanceTransaction;

import java.util.Scanner;

public class Main {
  public static void main(String[] args) throws Exception {
    // Initializes the resources needed.
    CqlSession session = CqlSession.builder().
        withKeyspace(CqlIdentifier.fromCql("wholesale")).
        build();
    Scanner scanner = new Scanner(System.in);
    System.out.println("The system has been started.");

    // Reads the input line-by-line.
    while (scanner.hasNext()) {
      String line = scanner.nextLine();
      String[] parameters = line.split(",");

      // Dynamically defines the transaction type and passes in the parameters.
      BaseTransaction transaction;
      switch (parameters[0]) {
      case "N":
        transaction = new NewOrderTransaction(session, parameters);
        break;
      case "P":
        transaction = new PaymentTransaction(session, parameters);
        break;
      case "D":
        transaction = new DeliveryTransaction(session, parameters);
        break;
      case "O":
        transaction = new OrderStatusTransaction(session, parameters);
        break;
      case "S":
        transaction = new StockLevelTransaction(session, parameters);
        break;
      case "I":
        transaction = new PopularItemTransaction(session, parameters);
        break;
      case "T":
        transaction = new TopBalanceTransaction(session, parameters);
        break;
      case "R":
        transaction = new RelatedCustomerTransaction(session, parameters);
        break;
      default:
        throw new Exception("Unknown transaction types");
      }

      // Reads the data lines.
      int numOfDataLines = transaction.numOfDataLines();
      String[] dataLines = new String[numOfDataLines];
      for (int i = 0; i < numOfDataLines; i++) {
        dataLines[i] = scanner.nextLine();
      }

      // Executes the transaction.
      transaction.execute(dataLines);
    }

    // Closes the opened resources.
    session.close();
    scanner.close();
  }
}
