package beam;

import info.javaspec.dsl.Cleanup;
import info.javaspec.dsl.Establish;
import info.javaspec.dsl.It;
import info.javaspec.runner.JavaSpecRunner;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.chaoslabs.beam.pipelines.JdbcReadAllExample1;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Map;

import static org.junit.Assert.assertEquals;

@RunWith(JavaSpecRunner.class)
public class JdbcReadAllExample1Tests {
  // Sanity Check (And each spec requires at least one test)
  It says_hello = () -> assertEquals("Hello World!", "Hello World!");


  Establish pipeline_configuration = () ->
      PipelineOptionsFactory.register(JdbcReadAllExample1.EnricherOptions.class);

  public class example_1_works_with {
    private final JdbcReadAllExample1.EnricherOptions options = PipelineOptionsFactory
        .fromArgs(
            "--jdbcDriver=" + Constants.JDBC_DRIVER,
            "--jdbcUrl=" + Constants.JDBC_URL
        ).as(JdbcReadAllExample1.EnricherOptions.class);
//        .create();

    @Rule
    public final transient TestPipeline pipeline = TestPipeline
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

      PCollection<String> output = JdbcReadAllExample1
          .buildPipeline(options, input);

      PAssert.that(output)
          .containsInAnyOrder(
              "{\"source_domain\":\"www.medium.com\",\"compensation\":0.01,\"id\":2}"
          );

      pipeline.run();
    };

    It multiple_entries = () -> {
      PCollection<String> input = pipeline
          .apply(Create.of(
              "http://www.medium.com",
              "https://www.blogspot.com")
          );

      PCollection<String> output = JdbcReadAllExample1
          .buildPipeline(options, input);

      PAssert.that(output)
          .containsInAnyOrder(
              "{\"source_domain\":\"www.medium.com\",\"compensation\":0.01,\"id\":2}",
              "{\"source_domain\":\"www.blogspot.com\",\"compensation\":0.03,\"id\":1}"
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

      PCollection<String> output = JdbcReadAllExample1
          .buildPipeline(options, input);

      PAssert.that(output)
          .containsInAnyOrder(
              "{\"source_domain\":\"www.medium.com\",\"compensation\":0.01,\"id\":2}",
              "{\"source_domain\":\"www.blogspot.com\",\"compensation\":0.03,\"id\":1}"
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

      PCollection<String> output = JdbcReadAllExample1
          .buildPipeline(options, input);

      BoundedWindow firstWindow = new IntervalWindow(baseTime, WINDOW_DURATION);
      BoundedWindow secondWindow = new IntervalWindow(baseTime.plus(WINDOW_DURATION), WINDOW_DURATION);

      String medium = "{\"source_domain\":\"www.medium.com\",\"compensation\":0.01,\"id\":2}";
      String blogspot = "{\"source_domain\":\"www.blogspot.com\",\"compensation\":0.03,\"id\":1}";

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