package edu.cs4224;

public class CqlQueryList {
  public static final String DISTRICT_INFO
          = "SELECT D_NEXT_O_ID, D_TAX FROM district WHERE D_W_ID = %d AND D_ID = %d";
  public static final String UPDATE_NEXT_ORDER_ID
          = "UPDATE district SET D_NEXT_O_ID = %d WHERE D_W_ID = %d AND D_ID = %d";
  public static final String CREATE_NEW_ORDER
          = "INSERT INTO customer_order (O_ID, O_D_ID, O_W_ID, O_C_ID, O_ENTRY_D, O_OL_CNT, O_ALL_LOCAL) " +
          "VALUES(%d, %d, %d, %d, '%s', %d, %d)";
  public static final String CHECK_STOCK_INFO
          = "SELECT S_QUANTITY, S_REMOTE_CNT, S_ORDER_CNT, S_YTD FROM stock WHERE S_W_ID = %d AND S_I_ID = %d";
  public static final String UPDATE_STOCK
          = "UPDATE stock SET S_QUANTITY=%f, S_YTD=%f, S_ORDER_CNT=%d, " +
          "S_REMOTE_CNT=%d WHERE S_W_ID = %d AND S_I_ID = %d";
  public static final String CHECK_ITEM_INFO
          = "SELECT I_PRICE, I_NAME FROM item WHERE I_ID = %d";
  public static final String CREATE_ORDER_LINE
          = "INSERT INTO order_line(OL_O_ID, OL_D_ID, OL_W_ID, OL_NUMBER, OL_I_ID, OL_SUPPLY_W_ID, OL_QUANTITY, OL_AMOUNT, OL_DIST_INFO)" +
          " VALUES(%d, %d, %d, %d, %d, %d, %d, %f, '%s')";
  public static final String CHECK_WAREHOUSE_TAX
          = "SELECT W_TAX FROM warehouse WHERE W_ID=%d";
  public static final String CHECK_CUSTOMER_INFO
          = "SELECT C_DISCOUNT, C_LAST, C_CREDIT FROM customer WHERE C_W_ID=%d, C_D_ID=%d, C_ID=%d";
}
