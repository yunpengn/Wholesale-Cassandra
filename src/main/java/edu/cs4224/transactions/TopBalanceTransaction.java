package edu.cs4224.transactions;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Row;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;


class CustomerInfo implements Comparable<CustomerInfo> {
    public double balance;
    public int warehouse_id;
    public int district_id;
    public int customer_id;
    public CustomerInfo(double balance, int warehouse_id, int district_id, int customer_id) {
        this.balance = balance;
        this.warehouse_id = warehouse_id;
        this.district_id = district_id;
        this.customer_id = customer_id;
    }

    @Override
    public int compareTo(CustomerInfo o) {
        if (balance > o.balance) return -1;
        else if (balance == o.balance) return 0;
        else return 1;
    }
}

public class TopBalanceTransaction extends BaseTransaction {

    private static final String SELECT_CUSTMER_ORDER_BY = "SELECT * FROM user_balance WHERE C_W_ID = %d ORDER BY C_BALANCE DESC LIMIT 10;";
    private static final String SELECT_CUSTOMER_INFO = "SELECT C_FIRST, C_LAST, C_MIDDLE FROM customer_r WHERE C_W_ID=%d AND C_D_ID=%d AND C_ID=%d";
    private static final String SELECT_WAREHOUSE_NAME = "SELECT W_NAME FROM warehouse WHERE W_ID=%d";
    private static final String SELECT_DISTRICT_NAME = "SELECT D_NAME FROM district_r WHERE D_W_ID=%d AND D_ID=%d";

    public TopBalanceTransaction(final CqlSession session, final String[] parameters) {
        super(session, parameters);
    }

    @Override
    public void execute(final String[] dataLines) {
        ArrayList<CustomerInfo> infos = new ArrayList<>();
        for (int i = 1; i <= 10; i++) {
            List<Row> res = executeQuery(String.format(SELECT_CUSTMER_ORDER_BY, i));
            for (Row row : res) {
                double balance = row.getBigDecimal("C_BALANCE").doubleValue();
                int did = row.getInt("C_D_ID");
                int cid = row.getInt("C_ID");
                infos.add(new CustomerInfo(balance, i, did, cid));
            }
        }
        Collections.sort(infos);

        for (int i = 0; i < 10; i++) {
            Row customer_info = executeQuery(String.format(SELECT_CUSTOMER_INFO, infos.get(i).warehouse_id, infos.get(i).district_id, infos.get(i).customer_id)).get(0);
            Row warehouse_info = executeQuery(String.format(SELECT_WAREHOUSE_NAME, infos.get(i).warehouse_id)).get(0);
            Row district_info = executeQuery(String.format(SELECT_DISTRICT_NAME, infos.get(i).warehouse_id, infos.get(i).district_id)).get(0);

            System.out.println(String.format("Customer #%d: Name(%s, %s, %s), Balance(%f), Warehouse(%s), District(%s)", i + 1, customer_info.getString("C_FIRST"),
                    customer_info.getString("C_MIDDLE"), customer_info.getString("C_LAST"), infos.get(i).balance, warehouse_info.getString("W_NAME"), district_info.getString("D_NAME")));
        }
    }
}
