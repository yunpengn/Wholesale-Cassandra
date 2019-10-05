package edu.cs4224;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Row;

import java.io.*;
import java.util.*;

import static edu.cs4224.ScalingParameters.*;

public class DataLoader implements Closeable {

    private CqlSession session;
    private final Map<Integer, Set<Integer>> districtIDs;

    public DataLoader() {
        session = CqlSession.builder()
                .withKeyspace(CqlIdentifier.fromCql("wholesale"))
                .build();
        districtIDs = new HashMap<>();

        File file = new File("data/temp");
        if (!file.exists()) {
            file.mkdirs();
        }
    }

    public void loadSchema() throws Exception {
        executeCommand("cqlsh -f src/main/resources/schema.cql --request-timeout=3600");
    }

    public void loadData() throws Exception {
        warehouse();
        district();
        customer();
        item();
        order_line();
        customer_order();
        stock();
        appendNextDeliveryID();
        addItemOrderList();
    }

    private void warehouse() throws Exception {
        executeCQLCommand(
                "USE wholesale",
                "COPY warehouse (W_ID, W_NAME, W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP, W_TAX, W_YTD) FROM './data/data-files/warehouse.csv' WITH DELIMITER=','"
        );
    }

    private void district() throws Exception {
        filePartitioner("district",
                (wWriter, data) -> {
                    addDistrict(data[0], data[1]);

                    scalingCounter(data, 10, SCALE_D_YTD);
                    scalingCounter(data, 11, SCALE_D_NEXT_O_ID);

                    String[] appendedData = new String[data.length + 1];
                    for (int i = 0; i < data.length; i++)
                        appendedData[i] = data[i];
                    appendedData[data.length] = "0";

                    wWriter.write(createCSVRow(appendedData, 1, 2, 10, 11, 12));
                }, (rWriter, data) -> {
                    rWriter.write(createCSVRow(data, 1, 2, 3, 4, 5, 6, 7, 8, 9));
                });

        executeCQLCommand(
                "USE wholesale",
                "COPY district_w (D_W_ID, D_ID, D_YTD, D_NEXT_O_ID, D_NEXT_DELIVERY_O_ID) FROM './data/temp/district_w.csv' WITH DELIMITER=','"
        );
        executeCQLCommand(
                "USE wholesale",
                "COPY district_r (D_W_ID, D_ID, D_NAME, D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP, D_TAX) FROM './data/temp/district_r.csv' WITH DELIMITER=','"
        );
    }

    private void customer() throws Exception {
        filePartitioner("customer",
                (wWriter, data) -> {
                    scalingCounter(data, 17, SCALE_C_BALANCE);
                    scalingCounter(data, 18, SCALE_C_YTD_PAYMENT);
                    scalingCounter(data, 19, SCALE_C_PAYMENT_CNT);
                    scalingCounter(data, 20, SCALE_C_DELIVERY_CNT);

                    wWriter.write(createCSVRow(data, 1, 2, 3, 17, 18, 19, 20));
                }, (rWriter, data) -> {
                    rWriter.write(createCSVRow(data, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 21));
                });

        executeCQLCommand(
                "USE wholesale",
                "COPY customer_w (C_W_ID, C_D_ID, C_ID, C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_DELIVERY_CNT) FROM './data/temp/customer_w.csv' WITH DELIMITER=','"
        );
        executeCQLCommand(
                "USE wholesale",
                "COPY customer_r (C_W_ID, C_D_ID, C_ID, C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, C_DATA) FROM './data/temp/customer_r.csv' WITH DELIMITER=','"
        );
    }

    private void item() throws Exception {
        executeCQLCommand(
                "USE wholesale",
                "COPY item (I_ID, I_NAME, I_PRICE, I_IM_ID, I_DATA) FROM './data/data-files/item.csv' WITH DELIMITER=','"
        );
    }

    private void order_line() throws Exception {
        try (
                BufferedReader reader = new BufferedReader(new FileReader("data/data-files/order-line.csv"));
                BufferedWriter writer = new BufferedWriter(new FileWriter("data/temp/order-line.csv"))
        ) {
            String row;
            while ((row = reader.readLine()) != null) {
                writer.write(row.replaceAll("null", "") + "\n");
            }
            writer.flush();
        }
        executeCQLCommand(
                "USE wholesale",
                "COPY order_line (OL_W_ID, OL_D_ID, OL_O_ID, OL_NUMBER, OL_I_ID, OL_DELIVERY_D, OL_AMOUNT, OL_SUPPLY_W_ID, OL_QUANTITY, OL_DIST_INFO) FROM './data/temp/order-line.csv'"
        );
    }

    private void customer_order() throws Exception {
        try (
                BufferedReader reader = new BufferedReader(new FileReader("data/data-files/order.csv"));
                BufferedWriter writer = new BufferedWriter(new FileWriter("data/temp/order.csv"))
        ) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] rowData = line.split(",");

                String query = "SELECT OL_NUMBER, OL_I_ID, OL_DELIVERY_D, OL_AMOUNT, OL_SUPPLY_W_ID, OL_QUANTITY FROM wholesale.order_line WHERE OL_W_ID = %s AND OL_D_ID = %s AND OL_O_ID = %s";
                List<Row> rows = session.execute(String.format(query, rowData[0], rowData[1], rowData[2])).all();

                OrderlineInfoMap infoMap = new OrderlineInfoMap(rows);

                writer.append(String.format("%s|%s\n", line.replace("null", "").replace(",", "|"), infoMap.toJson()));
            }
            writer.flush();
        }
        executeCQLCommand(
                "USE wholesale",
                "COPY customer_order (O_W_ID, O_D_ID, O_ID, O_C_ID, O_CARRIER_ID, O_OL_CNT, O_ALL_LOCAL, O_ENTRY_D, O_L_INFO) FROM './data/temp/order.csv' WITH DELIMITER='|'"
        );
    }

    private void stock() throws Exception {
        filePartitioner("stock",
                (wWriter, data) -> {
                    scalingCounter(data, 3, SCALE_S_QUANTITY);
                    scalingCounter(data, 4, SCALE_S_YTD);
                    scalingCounter(data, 5, SCALE_S_ORDER_CNT);
                    scalingCounter(data, 6, SCALE_S_REMOTE_CNT);

                    wWriter.write(createCSVRow(data, 1, 2, 3, 4, 5, 6));
                }, (rWriter, data) -> {
                    rWriter.write(createCSVRow(data, 1, 2, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17));
                });

        executeCQLCommand(
                "USE wholesale",
                "COPY stock_w (S_W_ID, S_I_ID, S_QUANTITY, S_YTD, S_ORDER_CNT, S_REMOTE_CNT) FROM './data/temp/stock_w.csv' WITH DELIMITER=','"
        );
        executeCQLCommand(
                "USE wholesale",
                "COPY stock_r (S_W_ID, S_I_ID, S_DIST_01, S_DIST_02, S_DIST_03, S_DIST_04, S_DIST_05, S_DIST_06, S_DIST_07, S_DIST_08, S_DIST_09, S_DIST_10, S_DATA) FROM './data/temp/stock_r.csv' WITH DELIMITER=','"
        );
    }

    private void appendNextDeliveryID() {
        for (Map.Entry<Integer, Set<Integer>> entry : districtIDs.entrySet()) {
            int C_W_ID = entry.getKey();
            for (int C_D_ID : entry.getValue()) {
                String query = "SELECT * FROM customer_order WHERE o_w_id = %d AND o_d_id = %d ORDER BY o_id";
                List<Row> orders = session.execute(String.format(query, C_W_ID, C_D_ID)).all();

                int min = Integer.MAX_VALUE;
                for (Row order : orders) {
                    if (order.isNull("o_carrier_id")) {
                        min = Math.min(min, order.getInt("O_ID"));
                    }
                }

                query = "UPDATE district_w SET D_NEXT_DELIVERY_O_ID = D_NEXT_DELIVERY_O_ID + %d WHERE D_W_ID = %d AND D_ID = %d";
                session.execute(String.format(query, min, C_W_ID, C_D_ID));
            }
        }
    }

    private void addItemOrderList() throws Exception {
        final String query = "UPDATE item SET i_o_id_list = {%s} WHERE i_id = %d";

        try (BufferedReader reader = new BufferedReader(new FileReader("data/temp/order.csv"))) {
            String row;
            Map<Integer, Set<String>> toOrderList = new HashMap<>();

            while ((row = reader.readLine()) != null) {
                String[] parts = row.split("|");
                String warehouseID = parts[0];
                String districtID = parts[1];
                String orderID = parts[2];
                String customerID = parts[3];
                String infoStr = String.format("'%s-%s-%s-%s'", warehouseID, districtID, orderID, customerID);

                OrderlineInfoMap infoMap = OrderlineInfoMap.fromJson(parts[8]);
                infoMap.values().stream().map(OrderlineInfo::getId).forEach(itemID -> {
                    Set<String> orderIDs = toOrderList.getOrDefault(itemID, new HashSet<>());
                    orderIDs.add(infoStr);
                    toOrderList.put(itemID, orderIDs);
                });
            }

            for (Map.Entry<Integer, Set<String>> entry: toOrderList.entrySet()) {
                StringJoiner joiner = new StringJoiner(",");
                for (String item: entry.getValue()) {
                    joiner.add(item);
                }

                session.execute(String.format(query, joiner.toString(), entry.getKey()));
            }
        }
    }

    private void addDistrict(String D_W_ID, String D_ID) {
        int d_w_id = Integer.parseInt(D_W_ID);
        int d_id = Integer.parseInt(D_ID);

        Set<Integer> set = districtIDs.getOrDefault(d_w_id, new HashSet<>());
        set.add(d_id);
        districtIDs.put(d_w_id, set);
    }

    private void scalingCounter(String[] data, int index, int scaleParam) {
        data[index - 1] = toDB(Double.parseDouble(data[index - 1]), scaleParam);
    }

    // colIndexes is an array of integers which indicate the index in data.
    // pls take care that index start from 1 rather than 0
    private String createCSVRow(String[] data, int... colIndexes) {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < colIndexes.length; i++) {
            int index = colIndexes[i] - 1;

            if (!data[index].equals("null")) {
                builder.append(data[index]);
            }
            if (i != colIndexes.length - 1) {
                builder.append(",");
            }
        }
        builder.append("\n");
        return builder.toString();
    }

    private void filePartitioner(
            String fileName,
            Utils.BiThrowingConsumer<BufferedWriter, String[]> wPartitionConsumer,
            Utils.BiThrowingConsumer<BufferedWriter, String[]> rPartitionConsumer) throws Exception {
        try (
                BufferedReader reader = new BufferedReader(new FileReader("data/data-files/" + fileName + ".csv"));
                BufferedWriter wWriter = new BufferedWriter(new FileWriter("data/temp/" + fileName + "_w.csv"));
                BufferedWriter rWriter = new BufferedWriter(new FileWriter("data/temp/" + fileName + "_r.csv"))
        ) {
            String row;
            while ((row = reader.readLine()) != null) {
                String[] rowData = row.split(",");

                wPartitionConsumer.accept(wWriter, rowData);
                rPartitionConsumer.accept(rWriter, rowData);
            }

            wWriter.flush();
            rWriter.flush();
        }
    }

    private void executeCQLCommand(String... cqls) throws Exception {
        try (
                BufferedWriter writer = new BufferedWriter(new FileWriter("data/temp/temp.cql"))
        ) {
            StringBuilder builder = new StringBuilder();
            for (String cql : cqls) {
                builder.append(cql);
                builder.append(";\n");
            }
            writer.write(builder.toString());
            writer.flush();
        }

        executeCommand("cqlsh -f data/temp/temp.cql --request-timeout=3600");
    }

    private void executeCommand(String command) throws Exception {
        Runtime rt = Runtime.getRuntime();
        Process proc = rt.exec(command);

        BufferedReader inReader = new BufferedReader(new InputStreamReader(proc.getInputStream()));
        BufferedReader errReader = new BufferedReader(new InputStreamReader(proc.getErrorStream()));

        String line;
        while ((line = inReader.readLine()) != null)
            System.out.println(line);

        while ((line = errReader.readLine()) != null)
            System.out.println(line);
        proc.waitFor();

        inReader.close();
        errReader.close();
    }

    private void cleanup() {
        File folder = new File("data/temp");

        File[] files = folder.listFiles();
        if (files != null) {
            for (File file : files) {
                file.delete();
            }
        }
    }

    @Override
    public void close() {
        cleanup();
        if (session != null)
            session.close();
    }
}
