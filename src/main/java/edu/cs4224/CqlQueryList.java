package edu.cs4224;

public class CqlQueryList {
  public static final String DISTRICT_NEXT_ORDER_ID
      = "Select D_NEXT_O_ID from district where D_W_ID = %d and D_ID = %d";
}
