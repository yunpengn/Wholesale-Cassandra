package edu.cs4224.transactions;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Row;

import java.util.Collections;
import java.util.List;

public class TopBalanceTransaction extends BaseTransaction {

    public static final String SELECT_CUSTMER_ORDER_BY = "SELECT * FROM customer PER PARTITION LIMIT 10";

    public static final String SELECT_WAREHOUSE_NAME_BY_ID = "SELECT W_NAME FROM warehouse WHERE W_ID = %d";

    public static final String SELECT_DISTRICT_NAME_BY_ID = "SELECT D_NAME FROM district WHERE D_W_ID = %d AND D_ID = %d";

    public TopBalanceTransaction(final CqlSession session, final String[] parameters) {
        super(session, parameters);
    }

    @Override
    public void execute(final String[] dataLines) {
        // find top 10 customers with maximum balance

//        List<Row> rows = executeQuery(SELECT_CUSTMER_ORDER_BY);
//        Collections.sort(rows, (o1, o2) -> {
//            double b1 = o1.getBigDecimal("C_BALANCE").doubleValue();
//            double b2 = o2.getBigDecimal("C_BALANCE").doubleValue();
//
//            return -1 * Double.compare(b1, b2);
//        });
//
//        for (int i = 0; i < 10 && rows.size() > i; i++) {
//            Row customer = rows.get(i);
//            String warehouseName = executeQuery(String.format(SELECT_WAREHOUSE_NAME_BY_ID,
//                    customer.getInt("C_W_ID"))).get(0).getString("W_NAME");
//            String districtName = executeQuery(String.format(SELECT_DISTRICT_NAME_BY_ID,
//                    customer.getInt("C_W_ID"), customer.getInt("C_D_ID"))).get(0).getString("D_NAME");
//
//            System.out.printf("C_FIRST: %s, C_MIDDLE: %s, C_LAST: %s, C_BALANCE: %.1f, W_NAME: %s, D_NAME: %s\n",
//                    customer.getString("C_FIRST"), customer.getString("C_MIDDLE"), customer.getString("C_LAST"),
//                    customer.getBigDecimal("C_BALANCE").doubleValue(), warehouseName, districtName);
//        }


    }
}
