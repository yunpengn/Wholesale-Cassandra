package edu.cs4224;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;

import java.util.List;

public class Main {
  public static void main(String[] args) {
    List<Row> result = executeQuery("select release_version from system.local");
    for (Row row: result) {
      System.out.println(row.getFormattedContents());
    }
  }

  private static List<Row> executeQuery(String query) {
    try (CqlSession session = CqlSession.builder().build()) {
      ResultSet resultSet = session.execute(query);
      return resultSet.all();
    }
  }
}
