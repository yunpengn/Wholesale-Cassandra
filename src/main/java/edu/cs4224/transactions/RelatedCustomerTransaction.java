package edu.cs4224.transactions;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Row;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class RelatedCustomerTransaction extends BaseTransaction {

    public static final String SELECT_ORDER_BY_CUSTOMER = "SELECT * FROM order WHERE O_W_ID = %d AND O_D_ID = %d AND O_C_ID = %d";

    public static final String SELECT_ORDER_LINE_BY_ORDER = "SELECT * FROM order_line WHERE OL_W_ID = %d AND OL_D_ID = %d AND OL_O_ID = %d";

    private final int C_W_ID;
    private final int C_D_ID;
    private final int C_ID;

    public RelatedCustomerTransaction(final CqlSession session, final String[] parameters) {
        super(session, parameters);

        C_W_ID = Integer.parseInt(parameters[0]);
        C_D_ID = Integer.parseInt(parameters[1]);
        C_ID = Integer.parseInt(parameters[2]);
    }

    @Override
    public void execute(final String[] dataLines) {
//        StringBuilder builder = new StringBuilder();
//        builder.append(String.format("1. C_W_ID: %d, C_D_ID: %d, C_ID: %d\n", C_W_ID, C_D_ID, C_ID));
//
//        // find all orders of the given customer
//        List<Row> orders = executeQuery(String.format(SELECT_ORDER_BY_CUSTOMER, C_W_ID, C_D_ID, C_ID));
//        // find all items of the given customer
//        Set<Integer> itemID = new HashSet<>();
//        for (Row order: orders) {
//            List<Row> orderlines = executeQuery(String.format(SELECT_ORDER_LINE_BY_ORDER, order.getInt("O_W_ID"), order.getInt("O_D_ID"), order.getInt("O_ID")));
//            for (Row orderline: orderlines) {
//                itemID.add(orderline.getInt("OL_I_ID"));
//            }
//        }

        // transverse the whole order-line table to see whether a customer has two items match the itemID set.

    }
}
