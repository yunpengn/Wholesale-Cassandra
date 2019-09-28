import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;

public class Main {
  public static void main(String[] args) {
    Row row = executeQuery("select release_version from system.local");
    System.out.println(row.getFormattedContents());
  }

  private static Row executeQuery(String query) {
    try (CqlSession session = CqlSession.builder().build()) {
      ResultSet resultSet = session.execute(query);
      return resultSet.one();
    }
  }
}
