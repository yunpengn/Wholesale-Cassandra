package edu.cs4224;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class StatisticsCalculator {

    private static final String TotalNumberOfTransaction = "Total number of transactions processed: (\\d+)";
    private static final String TotalElapsedTime = "Total elapsed time: (\\d+)s";
    private static final String TransactionThroughput = "Transaction throughput: (\\d+) per second";
    private static final String AverageTransactionLatency = "Average transaction latency: (\\d+)ms";
    private static final String MedianTransactionLatency = "Median transaction latency: (\\d+)ms";
    private static final String NinetyFivePercentileTransactionLatency = "95th percentile transaction latency: (\\d+)ms";
    private static final String NinetyNinePercentileTransactionLatency = "99th percentile transaction latency: (\\d+)ms";

    public static void main(String[] args) throws Exception {
        String path = args[0];
        int NC = Integer.parseInt(args[1]);
        new StatisticsCalculator().run(path, NC);
    }

    private void run(String logPath, int NC) throws Exception {
        int totalNumberOfTransaction = 0;
        int totalExecutionTime = -1;
        int transactionThroughput = 0;

        for (int i = 1; i <= NC; i++) {
            String log = fetchLog(logPath, i);

            totalNumberOfTransaction += regex(log, TotalNumberOfTransaction);
            totalExecutionTime = Math.max(totalExecutionTime, regex(log, TotalElapsedTime));
            transactionThroughput += regex(log, TransactionThroughput);
        }

        System.out.println("totalNumberOfTransaction: "+ totalNumberOfTransaction);
        System.out.println("totalExecutionTime: " + totalExecutionTime);
        System.out.println("transactionThroughput: " + (transactionThroughput * 1.0 / NC));
    }

    private String fetchLog(String logPath, int index) throws Exception {
        try (
                BufferedReader reader = new BufferedReader(new FileReader(String.format("%s/%d.err.log", logPath, index)))
        ) {
            final StringBuilder builder = new StringBuilder();
            while (reader.ready()) {
                builder.append(reader.readLine());
            }
            return builder.toString();
        }
    }

    private int regex(String content, String patternStr) {
        Pattern pattern = Pattern.compile(patternStr);
        Matcher match = pattern.matcher(content);
        match.find();
        return Integer.parseInt(match.group(1));
    }
}
