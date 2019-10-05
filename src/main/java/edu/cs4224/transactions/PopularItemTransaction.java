package edu.cs4224.transactions;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Row;
import edu.cs4224.OrderlineInfo;
import edu.cs4224.OrderlineInfoMap;

import java.util.*;

import static edu.cs4224.CqlQueryList.*;

public class PopularItemTransaction extends BaseTransaction {

    public static final String SELECT_ORDER = "SELECT O_ID, O_ENTRY_D, O_C_ID, O_L_INFO FROM customer_order WHERE O_D_ID = %d AND O_W_ID = %d AND O_ID IN (%s)";

    public static final String SELECT_CUSTOMER = "SELECT C_FIRST, C_MIDDLE, C_LAST FROM customer_r WHERE C_W_ID = %d AND C_D_ID = %d AND C_ID = %d";

    public static final String SELECT_ITEM = "SELECT I_ID, I_NAME FROM item WHERE I_ID IN (%s)";

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
        StringBuilder builder = new StringBuilder();
        builder.append(String.format("1. W_ID: %d, D_ID: %d\n", W_ID, D_ID));
        builder.append(String.format("2. L: %d\n", L));

        int N = executeQuery(String.format(DISTRICT_NEXT_O_ID, W_ID, D_ID))
                .get(0).getInt("D_NEXT_O_ID");

        StringJoiner joiner = new StringJoiner(",");
        for (int i = N - L; i < N; i++)
            joiner.add(String.valueOf(i));
        List<Row> S = executeQuery(String.format(SELECT_ORDER, D_ID, W_ID, joiner.toString()));

        builder.append("3.\n");
        Set<Integer> popularItemSet = new HashSet<>();
        List<Set<Integer>> popularItemsInEveryOrder = new ArrayList<>(S.size());
        Map<Integer, String> itemIDNameMap = new HashMap<>();
        for (Row order : S) {
            int O_ID = order.getInt("O_ID");
            builder.append(String.format("O_ID: %d, O_ENTRY_D: %s\n",
                    O_ID, order.getInstant("O_ENTRY_D").toString()));

            Row customer = executeQuery(String.format(SELECT_CUSTOMER, W_ID, D_ID, order.getInt("O_C_ID"))).get(0);
            builder.append(String.format("C_FIRST: %s, C_MIDDLE: %s, C_LAST: %s\n",
                    customer.getString("C_FIRST"), customer.getString("C_MIDDLE"), customer.getString("C_LAST")));

            OrderlineInfoMap orderInfo = OrderlineInfoMap.fromJson(order.getString("O_L_INFO"));

            double max = Integer.MIN_VALUE;
            for (OrderlineInfo info : orderInfo.values()) {
                max = Math.max(max, info.getQuantity());
            }

            Set<Integer> itemIDs = new HashSet<>();
            for (OrderlineInfo info : orderInfo.values()) {
                double quantity = info.getQuantity();
                if (quantity == max) {
                    popularItemSet.add(info.getId());
                    itemIDs.add(info.getId());
                }
            }

            joiner = new StringJoiner(",");
            for (int id: itemIDs)
                joiner.add(String.valueOf(id));

            List<Row> items = executeQuery(String.format(SELECT_ITEM, joiner.toString()));
            for (Row item: items) {
                String itemName = item.getString("I_NAME");
                itemIDNameMap.putIfAbsent(item.getInt("I_ID"), itemName);
                builder.append(String.format("I_NAME: %s, OL_QUANTITY: %.1f\n", itemName, max));
            }

            popularItemsInEveryOrder.add(itemIDs);
            builder.append("\n");
        }

        builder.append("4.\n");
        for (int itemID: popularItemSet) {
            int count = 0;
            for (Set<Integer> items: popularItemsInEveryOrder) {
                if (items.contains(itemID)) {
                    count++;
                }
            }
            builder.append(String.format("I_NAME: %s, percentage: %2.2f%%\n",
                    itemIDNameMap.getOrDefault(itemID, ""),
                    count * 100.0 / popularItemSet.size()));
        }

        System.out.println(builder.toString());
    }
}
