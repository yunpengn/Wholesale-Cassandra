package edu.cs4224;

/**
 *
 */
public class ScalingParameters {
  // District
  public static final int SCALE_D_YTD = 10000;
  public static final int SCALE_D_NEXT_O_ID = 1;

  // Customer
  public static final int SCALE_C_BALANCE = 100;
  public static final int SCALE_C_YTD_PAYMENT = 1000;
  public static final int SCALE_C_PAYMENT_CNT = 1;
  public static final int SCALE_C_DELIVERY_CNT = 1;

  // Stock
  public static final int SCALE_S_QUANTITY = 1;
  public static final int SCALE_S_YTD = 100;
  public static final int SCALE_S_ORDER_CNT = 1;
  public static final int SCALE_S_REMOTE_CNT = 1;

  public static double fromDB(final String input, final int scalingParam) {
    return Float.parseFloat(input) / scalingParam;
  }

  public static String toDB(final double input, final int scalingParam) {
    int num = (int) input * scalingParam;
    return String.valueOf(num);
  }
}
