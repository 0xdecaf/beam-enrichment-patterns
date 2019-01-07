package org.chaoslabs.beam.pipelines;

import com.google.gson.Gson;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.values.KV;
import org.joda.time.DateTime;

import java.net.URL;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class JdbcReadAllExample1 {
    public interface EnricherOptions extends PipelineOptions {
        @Validation.Required
        @Default.String("org.postgresql.Driver")
        String getJdbcDriver();
        void setJdbcDriver(String jdbcDriver);
        @Validation.Required
        String getJdbcUrl();
        void setJdbcUrl(String jdbcUrl);
        @Validation.Required
        String getSubscriptionPath();
        void setSubscriptionPath(String subscriptionPath);
        @Validation.Required
        String getTopicPath();
        void setTopicPath(String topicPath);
    }

    public static void main(String[] args) {

        EnricherOptions options =
                PipelineOptionsFactory.fromArgs(args).withValidation().create().as(EnricherOptions.class);

        Pipeline pipeline = Pipeline.create(options);

        pipeline
            .apply("Read URLs from PubSub", PubsubIO.readStrings().fromSubscription(options.getSubscriptionPath()))

            .apply("Take each url and lookup what the database knows.", JdbcIO.<String, String>readAll()
                .withDataSourceConfiguration(
                    JdbcIO.DataSourceConfiguration.create(
                        options.getJdbcDriver(),
                        options.getJdbcUrl()
                    )
                )
                    // A parameterized prepared statement retrieving the data we want.
                .withQuery("SELECT * FROM affiliate WHERE source_domain=?")
                // Use the incoming element to set parameters on the prepared statement
                .withParameterSetter((JdbcIO.PreparedStatementSetter<String>) (element, preparedStatement) -> {
                    // Parameter locations are 1 based :/
                    URL url = new URL(element);
                    preparedStatement.setString(1, url.getHost());
                })
                // Transform the resultset to Json.  Multiple results can be returned and Json is easy to serialize.
                .withRowMapper((JdbcIO.RowMapper<String>) resultSet -> {
                    ResultSetMetaData metadata = resultSet.getMetaData();
                    // Iterate through each column to output
                    Map<String, Object> row = IntStream
                        // Iterate through each column to output
                        .range(0, resultSet.getMetaData().getColumnCount())
                        // Output a pair with the column name and value.
                        .mapToObj(i -> {
                            // This should never fail as we're bound by the column count but this is the interface.
                            try {
                                return KV.of(metadata.getColumnName(i), resultSet.getObject(i));
                            } catch (SQLException ignored) { }
                            return null;
                        })
                        // Create a map of key/value pairs
                        .collect(Collectors.toMap(KV::getKey, KV::getValue));
                    // Append the timestamp to the output so analytics can have a date to work with.
                    row.put("timestamp", DateTime.now().getMillis());
                    // Create a Json dictionary and return it.
                    return new Gson().toJson(row);
                })
            )
            .apply("Write Enriched data to Pub/Sub", PubsubIO
                .writeStrings()
                .to(options.getTopicPath())
            );

        pipeline.run();
    }
}
