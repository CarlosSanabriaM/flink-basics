package org.myorg.quickstart.other;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.myorg.quickstart.utils.StreamExecutionEnvironmentType;
import org.myorg.quickstart.utils.Utils;


/**
 * MaxBy acts like a map. Always returns a value for each input event, even if the current value is lower than the max returned value.
 */
public class StreamingMaxBy {

    public static void main(String[] args) throws Exception {
        // Set up the streaming execution environment
        final StreamExecutionEnvironment env = Utils
                .getStreamExecutionEnvironment(StreamExecutionEnvironmentType.DEPENDS_ON_CONTEXT);

        // Create a DataStream that contains some tuples
        DataStream<Tuple2<String, Integer>> dataStream = env.fromElements(
                Tuple2.of("a", 2),
                Tuple2.of("a", 1),
                Tuple2.of("b", 2),
                Tuple2.of("a", 3),
                Tuple2.of("a", 1),
                Tuple2.of("b", 1)
        );

        // Apply Flink operators
        DataStream<Tuple2<String, Integer>> resultDataStream = dataStream
                .keyBy(tuple -> tuple.f0)
                .maxBy(1);

        // Print the DataStream
        resultDataStream.print();

        // Sink data
        //dataStream.addSink(...);

        // Execute program
        env.execute("Flink Streaming Job (MaxBy example)");
    }

}
