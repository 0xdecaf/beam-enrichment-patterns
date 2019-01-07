package beam;

import info.javaspec.dsl.Cleanup;
import info.javaspec.dsl.Establish;
import info.javaspec.dsl.It;
import info.javaspec.runner.JavaSpecRunner;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.PCollection;
import org.chaoslabs.beam.pipelines.JdbcReadAllExample2;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.runner.RunWith;

import java.sql.*;
import java.util.Map;

import static org.junit.Assert.assertEquals;

@RunWith(JavaSpecRunner.class)
public class JdbcReadAllExample2Tests {
  // Sanity Check (And each spec requires at least one test)
  It says_hello = () -> assertEquals("Hello World!", "Hello World!");


  Establish pipeline_configuration = () ->
      PipelineOptionsFactory.register(JdbcReadAllExample2.EnricherOptions.class);

  public class example_2_works_with {
    private final PipelineOptions options = PipelineOptionsFactory
        .fromArgs(
            "--jdbcDriver=" + Constants.JDBC_DRIVER,
            "--jdbcUrl=" + Constants.JDBC_URL
        )
        .create();

    @Rule
    private final transient TestPipeline pipeline = TestPipeline
        .fromOptions(options)
        .enableAbandonedNodeEnforcement(true);

    private Connection conn;

    // Create the enrichment database
    Establish enrichment_database = () -> {
      conn = DriverManager.getConnection(Constants.JDBC_URL);

      conn.createStatement().execute(Constants.DDL_DROP_IF);
      // Create the table
      conn.createStatement().execute(Constants.DDL_CREATE);

      for(Map<String, Object> row: Constants.ROW_DATA) {
        PreparedStatement pstmt = conn.prepareStatement(Constants.DML_INSERT);
        pstmt.setString(1, row.get("source_domain").toString());
        pstmt.setDouble(2, Double.parseDouble(row.get("compensation").toString()));
        pstmt.executeUpdate();
      }
    };
    Cleanup database = () -> conn.close();

    It a_single_entry = () -> {
      PCollection<String> input = pipeline
          .apply(Create.of(
              "http://www.medium.com")
          );

      PCollection<String> output = JdbcReadAllExample2
          .buildPipeline(pipeline.getOptions().as(JdbcReadAllExample2.EnricherOptions.class), input);

      PAssert.that(output)
          .containsInAnyOrder(
              "{\"source_domain\":\"www.medium.com\",\"id\":2.0,\"timestamp\":9223371950454775,\"url\":\"http://www.medium.com\"}"
          );

      pipeline.run();
    };

    It multiple_entries = () -> {
      PCollection<String> input = pipeline
          .apply(Create.of(
              "http://www.medium.com",
              "https://www.blogspot.com")
          );

      PCollection<String> output = JdbcReadAllExample2
          .buildPipeline(pipeline.getOptions().as(JdbcReadAllExample2.EnricherOptions.class), input);

      PAssert.that(output)
          .containsInAnyOrder(
              "{\"source_domain\":\"www.medium.com\",\"id\":2.0,\"timestamp\":9223371950454775,\"url\":\"http://www.medium.com\"}",
              "{\"source_domain\":\"www.blogspot.com\",\"id\":1.0,\"timestamp\":9223371950454775,\"url\":\"https://www.blogspot.com\"}"
          );

      pipeline.run();
    };

    // Some constants to make sense of things
    private final Instant baseTime = new Instant(0);
    private final Duration WINDOW_DURATION = Duration.standardMinutes(5);

    It streaming_data = () -> {
      PCollection<String> input = pipeline
          .apply(TestStream.create(StringUtf8Coder.of())

              .advanceWatermarkTo(baseTime)
              .addElements("http://www.medium.com")
              // Advance the window so that it closes
              .advanceWatermarkTo(baseTime.plus(WINDOW_DURATION).plus(Duration.standardMinutes(1)))
              .addElements("https://www.blogspot.com")

              .advanceWatermarkTo(baseTime.plus(WINDOW_DURATION).plus(WINDOW_DURATION))
              .advanceWatermarkToInfinity()
          )
          .apply(Window.into(FixedWindows.of(WINDOW_DURATION)));

      PCollection<String> output = JdbcReadAllExample2
          .buildPipeline(pipeline.getOptions().as(JdbcReadAllExample2.EnricherOptions.class), input);

      PAssert.that(output)

          .containsInAnyOrder(
              "{\"source_domain\":\"www.medium.com\",\"id\":2.0,\"timestamp\":299999,\"url\":\"http://www.medium.com\"}"
              ,"{\"source_domain\":\"www.blogspot.com\",\"id\":1.0,\"timestamp\":599999,\"url\":\"https://www.blogspot.com\"}"
          );

      pipeline.run().waitUntilFinish();
    };

    It windowed_streeaming_data = () -> {
      PCollection<String> input = pipeline
          .apply(TestStream.create(StringUtf8Coder.of())

              .advanceWatermarkTo(baseTime)
              .addElements("http://www.medium.com")
              // Advance the window so that it closes
              .advanceWatermarkTo(baseTime.plus(WINDOW_DURATION).plus(Duration.standardMinutes(1)))
              .addElements("https://www.blogspot.com")

              .advanceWatermarkTo(baseTime.plus(WINDOW_DURATION).plus(WINDOW_DURATION))
              .advanceWatermarkToInfinity()
          )
          .apply(Window.into(FixedWindows.of(WINDOW_DURATION)));

      PCollection<String> output = JdbcReadAllExample2
          .buildPipeline(pipeline.getOptions().as(JdbcReadAllExample2.EnricherOptions.class), input);

      BoundedWindow firstWindow = new IntervalWindow(baseTime, WINDOW_DURATION);
      BoundedWindow secondWindow = new IntervalWindow(baseTime.plus(WINDOW_DURATION), WINDOW_DURATION);

      String medium = "{\"source_domain\":\"www.medium.com\",\"id\":2.0,\"timestamp\":299999,\"url\":\"http://www.medium.com\"}";
      String blogspot = "{\"source_domain\":\"www.blogspot.com\",\"id\":1.0,\"timestamp\":599999,\"url\":\"https://www.blogspot.com\"}";

    // Why does the first window have blogspot?
    PAssert.that(output)
        .inWindow(firstWindow)
        .containsInAnyOrder(
            medium
        );


    // Why does the first window have blogspot?
      PAssert.that(output)
          .inWindow(secondWindow) // Just has blogspot
          .containsInAnyOrder(
              blogspot
          );


      pipeline.run().waitUntilFinish();
    };
  }
}