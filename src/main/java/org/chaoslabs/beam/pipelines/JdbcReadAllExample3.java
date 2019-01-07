package org.chaoslabs.beam.pipelines;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;

import java.lang.reflect.Type;
import java.net.URL;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/*
Consider using GroupIntoBatches with natural keys in order to batch enrich.
Natural keys are likely to be duplicated when working with event streams so batching
of multiple events can happen easily by applying GroupIntoBatches and/or Latest.perKey
 */
public class JdbcReadAllExample3 {
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

        PCollection<String> input = pipeline
            .apply("Read JsonData from PubSub", PubsubIO.readStrings().fromSubscription(options.getSubscriptionPath()));

      PCollection<String> output = buildPipeline(options, input);

      output.apply("Write Enriched data to Pub/Sub", PubsubIO
            .writeStrings()
            .to(options.getTopicPath()));
        pipeline.run();
    }

  public static PCollection<String> buildPipeline(EnricherOptions options, PCollection<String> input) {
    PCollection<KV<String, String>> incomingData = input
        .apply("Apply Random Key", MapElements
            .via(new SimpleFunction<String, KV<String, String>>() {
                public KV<String, String> apply(String url) {
                    return KV.of(UUID.randomUUID().toString(), url);
                }
            })
        );

    PCollection<KV<String, String>> enrichedData = incomingData
        .apply("Take each url and lookup what the database knows.", JdbcIO.<KV<String, String>, KV<String, String>>readAll()
            .withDataSourceConfiguration(
                JdbcIO.DataSourceConfiguration.create(
                    options.getJdbcDriver(),
                    options.getJdbcUrl()
                )
            )
            // A parameterized prepared statement retrieving the data we want.
            // Note the use of the __key in the projection, we will use this to reconstruct the KV object so we an CoGroupByKey
            .withQuery("SELECT ? as __key, * FROM affiliate WHERE source_domain=?")
            // Use the incoming element to set parameters on the prepared statement
            .withParameterSetter((JdbcIO.PreparedStatementSetter<KV<String, String>>) (element, preparedStatement) -> {
              URL url = new URL(element.getValue());
              preparedStatement.setString(1, element.getKey());
              preparedStatement.setString(2, url.getHost());
            })
            // Transform the resultset to Json.  Multiple results can be returned and Json is easy to serialize.
            .withRowMapper((JdbcIO.RowMapper<KV<String, String>>) resultSet -> {
                String key = resultSet.getString("__key");
                ResultSetMetaData metadata = resultSet.getMetaData();
                // Iterate through each column to output
                Map<String, Object> row = IntStream
                    // Iterate through each column to output
                    .range(0, resultSet.getMetaData().getColumnCount())
                    .filter(i -> {
                        try {
                            return !metadata.getColumnName(i).equals("__key");
                        } catch (SQLException e) {
                            return false;
                        }
                    })
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

                // Create a Json dictionary and return it.
                return KV.of(key, new Gson().toJson(row));
            })
            .withCoder(KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()))
        );

    GroupByKey.applicableTo(enrichedData);
    // In order to use CoGroupByKey we need a TupleTag to mark the type of the Value side.
    TupleTag<String> ORIGINAL_TAG = new TupleTag<>();
    TupleTag<String> ENRICHED_TAG = new TupleTag<>();


    return KeyedPCollectionTuple
        // Mark the two PCollections with the appropriate TupleTags
        .of(ORIGINAL_TAG, incomingData)
        .and(ENRICHED_TAG, enrichedData)
        // Join using the internal CoGroupByKey
        .apply("Combine Streams", CoGroupByKey.create())

        .apply("Compose Final Object", ParDo.of(new DoFn<KV<String, CoGbkResult>, String>(){
            @ProcessElement
            public void processElement(ProcessContext c) {
                // At this point, we have our original data and all enriched records available to process.
                // If there are more than one record and getOnly is used, an exception will be raised.
                // It's critical that the same key isn't used in the same window otherwise we will have duplicate
                // original records.  When we explore natural keys for the pipeline we'll revisit this.
                String originalUrl = c.element().getValue().getOnly(ORIGINAL_TAG);

                /* The output of the SQL query can return multiple records. */
                // Iterable<String> jsonRecords = c.element().getValue().getAll(ENRICHED_TAG);

                // In our case, we know there is just a single record because we're using a unique key when querying the database.
                // The original URL is also available in the JSON data so we can skip combining these two datasets.
                // c.output(c.element().getValue().getOnly(ENRICHED_TAG));

                // In our case, we know there is just a single record because we're using a unique key when querying the database.
                // Read the JSON to add the original data
                Type type = new TypeToken<Map<String, Object>>(){}.getType();
                Gson gson = new Gson();

                // If there are no results, this will throw an exception.
                // If there are more than one result, this will throw an exception.
                Map<String, Object> data = gson.fromJson(c.element().getValue().getOnly(ENRICHED_TAG), type);

                // Append the timestamp to the output so analytics can have a date to work with.
                data.put("timestamp", c.timestamp().getMillis());
                data.put("url", originalUrl);
                String output = gson.toJson(data);
                c.output(output);

            }
        }));
  }
}
