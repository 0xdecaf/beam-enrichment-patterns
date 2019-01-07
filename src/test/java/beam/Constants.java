package beam;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;

public class Constants {
  /*
    Our scenario involves an external data source which we will mock here.
    Using a shared memory sqlite database means basic integration tests can be
    run without external dependencies.
   */
  public static final String JDBC_URL = "jdbc:sqlite:file::memory:?cache=shared";
  public static final String JDBC_DRIVER = "org.sqlite.JDBC";
  public static final String DDL_DROP_IF = "DROP TABLE IF EXISTS affiliate;";
  public static final String DDL_CREATE = "CREATE TABLE affiliate (\n"
      + " id integer PRIMARY KEY, \n"
      + " source_domain varchar(256) NOT NULL, \n"
      + " compensation double\n"
      + ");";
  public static final String DML_INSERT =
      "INSERT INTO affiliate(source_domain, compensation) VALUES(?, ?)";
  public static final List<Map<String, Object>> ROW_DATA = ImmutableList
      .of(ImmutableMap.of(
          "source_domain", "www.blogspot.com",
          "compensation", 0.03
          ),
          ImmutableMap.of(
              "source_domain", "www.medium.com",
              "compensation", 0.01
          )
      );
}
