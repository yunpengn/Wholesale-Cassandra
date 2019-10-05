package edu.cs4224.transactions;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Row;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static edu.cs4224.CqlQueryList.*;

public class PopularItemTransaction extends BaseTransaction {

    public static final String SELECT_ORDER = "SELECT O_ID, O_ENTRY_D, O_C_ID FROM customer_order WHERE O_D_ID = %d AND O_W_ID = %d AND O_ID = %d";

    public static final String SELECT_ORDER_LINE = "SELECT * FROM order_line WHERE OL_O_ID = %d AND OL_D_ID = %d AND OL_W_ID = %d";

    public static final String SELECT_CUSTOMER = "SELECT * FROM customer WHERE C_W_ID = %d AND C_D_ID = %d AND C_ID = %d";

    public static final String SELECT_ITEM = "SELECT I_NAME FROM item WHERE I_ID = %d";

    private final int W_ID;
    private final int D_ID;
    private final int L;

    public PopularItemTransaction(final CqlSession session, final String[] parameters) {
        super(session, parameters);

        W_ID = Integer.parseInt(parameters[1]);
        D_ID = Integer.parseInt(parameters[2]);
        L = Integer.parseInt(parameters[3]);
    }

    @Override
    public void execute(final String[] dataLines) {
//        StringBuilder builder = new StringBuilder();
//        builder.append(String.format("1. W_ID: %d, D_ID: %d\n", W_ID, D_ID));
//        builder.append(String.format("2. L: %d\n", L));
//
//        int N = executeQuery(String.format(DISTRICT_NEXT_O_ID, W_ID, D_ID))
//                .get(0).getInt("D_NEXT_O_ID");
//
//        List<Row> S = new ArrayList<>(L);
//        for (int i = L; i > 0; i--) {
//            List<Row> orders = executeQuery(String.format(SELECT_ORDER, D_ID, W_ID, N - i));
//            if (!orders.isEmpty()) {
//                S.add(orders.get(0));
//            }
//        }
//
//        builder.append("3.\n");
//        Set<String> popularItemSet = new HashSet<>();
//        List<Set<String>> popularItemsInEveryOrder = new ArrayList<>(S.size());
//        for (Row order : S) {
//            int O_ID = order.getInt("O_ID");
//            builder.append(String.format("O_ID: %d, O_ENTRY_D: %s\n",
//                    O_ID, order.getLocalTime("O_ENTRY_D").toString()));
//
//            Row customer = executeQuery(String.format(SELECT_CUSTOMER, W_ID, D_ID, order.getInt("O_C_ID"))).get(0);
//            builder.append(String.format("C_FIRST: %s, C_MIDDLE: %s, C_LAST: %s\n",
//                    customer.getString("C_FIRST"), customer.getString("C_MIDDLE"), customer.getString("C_LAST")));
//
//            List<Row> orderLines = executeQuery(String.format(SELECT_ORDER_LINE, O_ID, D_ID, W_ID));
//            if (orderLines.isEmpty())
//                continue;
//
//            double max = orderLines.get(0).getBigDecimal("OL_QUANTITY").doubleValue();
//            for (int i = 1; i < orderLines.size(); i++) {
//                double quantity = orderLines.get(i).getBigDecimal("OL_QUANTITY").doubleValue();
//                if (max < quantity) {
//                    max = quantity;
//                }
//            }
//
//            Set<String> items = new HashSet<>();
//            for (Row orderLine: orderLines) {
//                double quantity = orderLine.getDouble("OL_QUANTITY");
//                if (quantity == max) {
//                    Row item = executeQuery(String.format(SELECT_ITEM, orderLine.getInt("OL_I_ID"))).get(0);
//
//                    String itemName = item.getString("I_NAME");
//                    popularItemSet.add(itemName);
//                    items.add(itemName);
//
//                    builder.append(String.format("I_NAME: %s, OL_QUANTITY: %.1f\n", itemName, quantity));
//                }
//            }
//            popularItemsInEveryOrder.add(items);
//            builder.append("\n");
//        }
//
//        builder.append("4.\n");
//        for (String itemName: popularItemSet) {
//            int count = 0;
//            for (Set<String> items: popularItemsInEveryOrder) {
//                if (items.contains(itemName)) {
//                    count++;
//                }
//            }
//            builder.append(String.format("I_NAME: %s, percentage: %2.2f%%\n", itemName, count * 100.0 / popularItemSet.size()));
//        }
//
//        System.out.println(builder.toString());
    }
}
