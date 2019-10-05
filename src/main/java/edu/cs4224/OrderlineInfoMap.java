package edu.cs4224;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.datastax.oss.driver.api.core.cql.Row;

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
        return JSON.toJSONString(this);
    }

    public static OrderlineInfoMap fromJson(String json) {
        return (OrderlineInfoMap) JSON.parseObject(json, new TypeReference<HashMap<Integer, OrderlineInfo>>() {});
    }
}
