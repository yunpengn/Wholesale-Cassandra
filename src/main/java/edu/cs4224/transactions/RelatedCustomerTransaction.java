package edu.cs4224.transactions;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Row;
import edu.cs4224.OrderlineInfo;
import edu.cs4224.OrderlineInfoMap;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class RelatedCustomerTransaction extends BaseTransaction {

    public static final String SELECT_ORDERS = "SELECT O_L_INFO FROM customer_order WHERE O_W_ID = %d AND O_D_ID = %d AND O_C_ID = %d ALLOW FILTERING";

    public static final String SELECT_ITEMS = "SELECT I_O_ID_LIST FROM item WHERE I_ID IN (%s)";

    private final int C_W_ID;
    private final int C_D_ID;
    private final int C_ID;

    public RelatedCustomerTransaction(final CqlSession session, final String[] parameters) {
        super(session, parameters);

        C_W_ID = Integer.parseInt(parameters[1]);
        C_D_ID = Integer.parseInt(parameters[2]);
        C_ID = Integer.parseInt(parameters[3]);
    }

    @Override
    public void execute(final String[] dataLines) {
        StringBuilder builder = new StringBuilder();
        builder.append(String.format("1. C_W_ID: %d, C_D_ID: %d, C_ID: %d\n", C_W_ID, C_D_ID, C_ID));

        List<Row> orders = executeQuery(String.format(SELECT_ORDERS, C_W_ID, C_D_ID, C_ID));
        for (Row order: orders) {
            OrderlineInfoMap infoMap = OrderlineInfoMap.fromJson(order.getString("O_L_INFO"));
            Set<Integer> givenCustomerItems = infoMap.values().stream().map(OrderlineInfo::getId).collect(Collectors.toSet());

            StringJoiner joiner = new StringJoiner(",");
            givenCustomerItems.forEach(itemID -> joiner.add(String.valueOf(itemID)));

            List<String> result = new ArrayList<>();
            List<Row> itemsOrdersList = executeQuery(String.format(SELECT_ITEMS, joiner.toString()));

            for (Row itemOrders: itemsOrdersList) {
                Set<String> ordersSet = itemOrders.getSet("I_O_ID_LIST", String.class);

                if (ordersSet == null)
                    continue;

                Set<String> checkSet = new HashSet<>();

                for (String orderInfo: ordersSet) {
                    System.out.println(orderInfo);
                    
                    String[] infos = orderInfo.split("-");
                    int warehoseID = Integer.parseInt(infos[0]);
                    int districtID = Integer.parseInt(infos[1]);
                    int orderID = Integer.parseInt(infos[2]);
                    String customerID = infos[3];

                    String key = String.format("%s%s%s", warehoseID, districtID, orderID);

                    if (warehoseID == C_W_ID)
                        continue;

                    if (checkSet.contains(key)) {
                        result.add(customerID);
                    } else {
                        checkSet.add(key);
                    }
                }
            }

            builder.append(result);
            System.out.println(builder.toString());
        }
    }
}
