package edu.cs4224;

import com.datastax.oss.driver.api.core.cql.Row;

public class OrderlineInfo {
    // OL_I_ID int,
    // OL_DELIVERY_D timestamp,
    // OL_AMOUNT decimal,
    // OL_SUPPLY_W_ID int,
    // OL_QUANTITY decimal,

    private int id;
    private String delivery;
    private double amount;
    private int supply;
    private double quantity;

    public OrderlineInfo(int id, String delivery, double amount, int supply, double quantity) {
        this.id = id;
        this.delivery = delivery;
        this.amount = amount;
        this.supply = supply;
        this.quantity = quantity;
    }

    public OrderlineInfo(Row row) {
        id = row.getInt("OL_I_ID");
        delivery = row.getInstant("OL_DELIVERY_D") != null ? row.getInstant("OL_DELIVERY_D").toString() : "";
        amount = row.getBigDecimal("OL_AMOUNT").doubleValue();
        supply = row.getInt("OL_SUPPLY_W_ID");
        quantity = row.getBigDecimal("OL_QUANTITY").doubleValue();
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getDelivery() {
        return delivery;
    }

    public void setDelivery(String delivery) {
        this.delivery = delivery;
    }

    public double getAmount() {
        return amount;
    }

    public void setAmount(double amount) {
        this.amount = amount;
    }

    public int getSupply() {
        return supply;
    }

    public void setSupply(int supply) {
        this.supply = supply;
    }

    public double getQuantity() {
        return quantity;
    }

    public void setQuantity(double quantity) {
        this.quantity = quantity;
    }
}
