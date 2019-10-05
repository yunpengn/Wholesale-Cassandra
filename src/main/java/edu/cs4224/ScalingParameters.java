package edu.cs4224;

/**
 *
 */
public class ScalingParameters {
  // District
  public static final int SCALE_D_YTD = 100;
  public static final int SCALE_D_NEXT_O_ID = 1;

  // Stock
  public static final int SCALE_S_QUANTITY = 1;
  public static final int SCALE_S_YTD = 100;
  public static final int SCALE_S_ORDER_CNT = 1;
  public static final int SCALE_S_REMOTE_CNT = 1;

  public static double fromDB(final long input, final int scalingParam) {
    return 1.0 * input / scalingParam;
  }

  public static String toDB(final double input, final int scalingParam) {
    long num = (long) input * scalingParam;
    return String.valueOf(num);
  }
}
