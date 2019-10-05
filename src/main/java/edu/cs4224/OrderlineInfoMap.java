package edu.cs4224;

import com.datastax.oss.driver.api.core.cql.Row;
import com.google.gson.Gson;

import java.util.HashMap;
import java.util.List;

public class OrderlineInfoMap extends HashMap<Integer, OrderlineInfo> {
    public OrderlineInfoMap() {
    }

    public OrderlineInfoMap(List<Row> rows) {
        for (Row row: rows) {
            this.put(row.getInt("OL_NUMBER"), new OrderlineInfo(row));
        }
    }

    public String toJson() {
        return new Gson().toJson(this);
    }

    public static OrderlineInfoMap fromJson(String json) {
        return new Gson().fromJson(json, OrderlineInfoMap.class);
    }
}
