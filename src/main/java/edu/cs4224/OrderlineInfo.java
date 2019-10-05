package edu.cs4224;

import com.datastax.oss.driver.api.core.cql.Row;

import java.util.Date;

public class OrderlineInfo {
    // OL_I_ID int,
    // OL_DELIVERY_D timestamp,
    // OL_AMOUNT decimal,
    // OL_SUPPLY_W_ID int,
    // OL_QUANTITY decimal,

    private int I_ID;
    private String DELIVERY_D;
    private double AMOUNT;
    private int SUPPLY_W_ID;
    private double QUANTITY;

    public OrderlineInfo(int I_ID, String DELIVERY_D, double AMOUNT, int SUPPLY_W_ID, double QUANTITY) {
        this.I_ID = I_ID;
        this.DELIVERY_D = DELIVERY_D;
        this.AMOUNT = AMOUNT;
        this.SUPPLY_W_ID = SUPPLY_W_ID;
        this.QUANTITY = QUANTITY;
    }

    public OrderlineInfo(Row row) {
        I_ID = row.getInt("OL_I_ID");
        DELIVERY_D = row.getInstant("OL_DELIVERY_D").toString();
        AMOUNT = row.getBigDecimal("OL_AMOUNT").doubleValue();
        SUPPLY_W_ID = row.getInt("OL_SUPPLY_W_ID");
        QUANTITY = row.getBigDecimal("OL_QUANTITY").doubleValue();
    }

    public int getI_ID() {
        return I_ID;
    }

    public void setI_ID(int i_ID) {
        I_ID = i_ID;
    }

    public String getDELIVERY_D() {
        return DELIVERY_D;
    }

    public void setDELIVERY_D(String DELIVERY_D) {
        this.DELIVERY_D = DELIVERY_D;
    }

    public double getAMOUNT() {
        return AMOUNT;
    }

    public void setAMOUNT(double AMOUNT) {
        this.AMOUNT = AMOUNT;
    }

    public int getSUPPLY_W_ID() {
        return SUPPLY_W_ID;
    }

    public void setSUPPLY_W_ID(int SUPPLY_W_ID) {
        this.SUPPLY_W_ID = SUPPLY_W_ID;
    }

    public double getQUANTITY() {
        return QUANTITY;
    }

    public void setQUANTITY(double QUANTITY) {
        this.QUANTITY = QUANTITY;
    }
}
