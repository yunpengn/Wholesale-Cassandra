package edu.cs4224;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;

import edu.cs4224.transactions.BaseTransaction;
import edu.cs4224.transactions.DeliveryTransaction;
import edu.cs4224.transactions.FinalStateTransaction;
import edu.cs4224.transactions.NewOrderTransaction;
import edu.cs4224.transactions.OrderStatusTransaction;
import edu.cs4224.transactions.PaymentTransaction;
import edu.cs4224.transactions.PopularItemTransaction;
import edu.cs4224.transactions.RelatedCustomerTransaction;
import edu.cs4224.transactions.StockLevelTransaction;
import edu.cs4224.transactions.TopBalanceTransaction;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;

public class Main {
  public static void main(String[] args) {
    try {
      new Main().init(args);
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(1);
    }
  }

  public void init(String[] args) throws Exception {
    if (args.length != 0) {
      switch (args[0]) {
      case "createschema":
        System.out.println("start to load schema");
        try (DataLoader loader = new DataLoader()) {
          loader.loadSchema();
        }
        break;
      case "loaddata":
        System.out.println("start to load data");
        try (DataLoader loader = new DataLoader()) {
          loader.loadData();
        }
        break;
      case "st":
        new StatisticsCalculator().run(args[1], Integer.parseInt(args[2]));
        break;
      case "strip":
        new StatisticsCalculator().stripStatisticLog(args[1]);
        break;
      case "run":
        run(args);
        break;
      default:
        throw new Exception("Unknown command");
      }
    }
  }

  public void run(String[] args) throws Exception {
    String consistencyLevel = args[1];
    // Initializes the resources needed.
    CqlSession session = CqlSession.builder().
        withKeyspace(CqlIdentifier.fromCql("wholesale")).
        build();
    Scanner scanner = new Scanner(System.in);
    System.out.println("The system has been started with consistency level " + consistencyLevel);

    // Some variables for statistics.
    List<Long> latency = new ArrayList<>();
    long start, end, txStart, txEnd, elapsedTime;

    // Reads the input line-by-line.
    start = System.nanoTime();
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

      transaction.setConsistencyLevel(consistencyLevel);

      // Reads the data lines.
      int numOfDataLines = transaction.numOfDataLines();
      String[] dataLines = new String[numOfDataLines];
      for (int i = 0; i < numOfDataLines; i++) {
        dataLines[i] = scanner.nextLine();
      }

      // Executes the transaction.
      txStart = System.nanoTime();
      System.out.println("\n======================================================================");
      System.out.printf("Transaction ID: %d\n", latency.size());
      transaction.execute(dataLines);
      System.out.println("======================================================================");
      txEnd = System.nanoTime();

      // Updates the statistics.
      elapsedTime = txEnd - txStart;
      latency.add(elapsedTime);
    }
    end = System.nanoTime();

    // Generates the performance report.
    elapsedTime = TimeUnit.SECONDS.convert(end - start, TimeUnit.NANOSECONDS);
    generatePerformanceReport(latency, elapsedTime);

    // Generates the final state report.
    BaseTransaction transaction = new FinalStateTransaction(session, new String[0]);
    transaction.execute(new String[0]);

    // Closes the opened resources.
    session.close();
    scanner.close();
  }

  private void generatePerformanceReport(List<Long> latency, long totalTime) {
    // Some magic.
    totalTime = Math.max(totalTime, 1);

    // Performs some mathematics here.
    Collections.sort(latency);
    int count = latency.size();
    long sum = latency.stream().mapToLong(a -> a).sum();

    System.err.println("\n======================================================================");
    System.err.println("Performance report: ");
    System.err.printf("Total number of transactions processed: %d\n", count);
    System.err.printf("Total elapsed time: %ds\n", totalTime);
    System.err.printf("Transaction throughput: %d per second\n", count / totalTime);
    System.err.printf("Average transaction latency: %dms\n", toMs(sum / count));
    System.err.printf("Median transaction latency: %dms\n", toMs(getMedian(latency)));
    System.err.printf("95th percentile transaction latency: %dms\n", toMs(getPercentile(latency, 95)));
    System.err.printf("99th percentile transaction latency: %dms\n", toMs(getPercentile(latency, 99)));
    System.err.println("======================================================================");
  }

  private long toMs(long nanoSeconds) {
    return TimeUnit.MILLISECONDS.convert(nanoSeconds, TimeUnit.NANOSECONDS);
  }

  private long getMedian(List<Long> list) {
    long mid = list.get(list.size() / 2);
    if (list.size() % 2 != 0) {
      return mid;
    } else {
      long mid2 = list.get(list.size() / 2 - 1);
      return (mid + mid2) / 2;
    }
  }

  /**
   * Assumes the input list is already sorted.
   */
  private long getPercentile(List<Long> list, int percentile) {
    int i = list.size() * percentile / 100;
    return list.get(i);
  }
}
