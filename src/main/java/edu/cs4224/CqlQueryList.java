package edu.cs4224;

public class CqlQueryList {
  // New order transaction queries
  public static final String DISTRICT_INFO
          = "SELECT D_TAX FROM district_r WHERE D_W_ID = %d AND D_ID = %d";
  public static final String DISTRICT_NEXT_O_ID
          = "SELECT D_NEXT_O_ID FROM district_w WHERE D_W_ID = %d AND D_ID = %d";
  public static final String UPDATE_NEXT_ORDER_ID
          = "UPDATE district_w SET D_NEXT_O_ID = D_NEXT_O_ID + 1 WHERE D_W_ID = %d AND D_ID = %d";
  public static final String CREATE_NEW_ORDER
          = "INSERT INTO customer_order (O_ID, O_D_ID, O_W_ID, O_C_ID, O_ENTRY_D, O_OL_CNT, O_ALL_LOCAL, O_L_INFO) " +
          "VALUES(%d, %d, %d, %d, '%s', %d, %d, %s)";
  public static final String CHECK_STOCK_INFO
          = "SELECT S_QUANTITY FROM stock_w WHERE S_W_ID = %d AND S_I_ID = %d";
  public static final String UPDATE_STOCK
          = "UPDATE stock_w SET S_QUANTITY=%f, S_YTD=S_YTD+%f, S_ORDER_CNT=S_ORDER_CNT+%d, " +
          "S_REMOTE_CNT=S_REMOTE_CNT+%d WHERE S_W_ID = %d AND S_I_ID = %d";
  public static final String CHECK_ITEM_INFO
          = "SELECT I_PRICE, I_NAME FROM item WHERE I_ID = %d";
  public static final String CREATE_ORDER_LINE
          = "INSERT INTO order_line(OL_O_ID, OL_D_ID, OL_W_ID, OL_NUMBER, OL_I_ID, OL_SUPPLY_W_ID, OL_QUANTITY, OL_AMOUNT, OL_DIST_INFO)" +
          " VALUES(%d, %d, %d, %d, %d, %d, %d, %f, '%s')";
  public static final String CHECK_WAREHOUSE_TAX
          = "SELECT W_TAX FROM warehouse WHERE W_ID=%d";
  public static final String CHECK_CUSTOMER_INFO
          = "SELECT C_DISCOUNT, C_LAST, C_CREDIT FROM customer_r WHERE C_W_ID=%d AND C_D_ID=%d AND C_ID=%d";

  // Payment transaction queries

  public static final String GET_WAREHOUSE_INFO
          = "SELECT W_YTD, W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP FROM warehouse WHERE W_ID = %d";
  public static final String UPDATE_WAREHOUSE_YTD
          = "UPDATE warehouse SET W_YTD=%f WHERE W_ID = %d";
  public static final String GET_DISTRICT_INFO
          = "SELECT D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP FROM district_r WHERE D_W_ID=%d AND D_ID=%d";
  public static final String GET_CUSTOMER_INFO
          = "SELECT * FROM customer_r WHERE C_W_ID=%d AND C_D_ID=%d AND C_ID=%d";
  public static final String UPDATE_DISTRICT_YTD
          = "UPDATE district_w SET D_YTD=D_YTD+%f WHERE D_W_ID=%d AND D_ID=%d";
  public static final String UPDATE_CUSTOMER_INFO
          = "UPDATE customer_w SET C_BALANCE=C_BALANCE - %f, C_YTD_PAYMENT=C_YTD_PAYMENT + %f, C_PAYMENT_CNT=C_PAYMENT_CNT + 1 WHERE C_W_ID=%d AND C_D_ID=%d AND C_ID=%d";
  public static final String GET_CUSTOMER_BALANCE
          = "SELECT C_BALANCE FROM customer_w WHERE C_W_ID=%d AND C_D_ID=%d AND C_ID=%d";
}
