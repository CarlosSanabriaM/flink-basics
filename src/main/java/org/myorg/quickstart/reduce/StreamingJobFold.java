package org.myorg.quickstart.reduce;

import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.myorg.quickstart.utils.StreamExecutionEnvironmentType;
import org.myorg.quickstart.utils.Utils;


/**
 * IMPORTANT: fold is deprecated.
 * <br><br>
 * Fold is like a reduce function, but with the possibility of returning a different type than the input type.
 * For that reason, the accumulator value must be initialized explicitly (in the reduce function,
 * the accumulator didn't need to be initialized, because the return type is the same as the input type,
 * and therefore, when the first event arrives, that event is returned and the accumulator is initialized with it).
 */
public class StreamingJobFold {

    public static void main(String[] args) throws Exception {
        // Set up the streaming execution environment
        final StreamExecutionEnvironment env = Utils
                .getStreamExecutionEnvironment(StreamExecutionEnvironmentType.DEPENDS_ON_CONTEXT);

        // Create a DataStream that contains some tuples
        DataStream<Tuple2<String, Integer>> dataStream = env.fromElements(
                // a
                Tuple2.of("a", 119), // w
                Tuple2.of("a", 111), // o
                Tuple2.of("a", 114), // r
                Tuple2.of("a", 108), // l
                Tuple2.of("a", 100), // d
                Tuple2.of("a", 33),  // !
                // b
                Tuple2.of("b", 116), // t
                Tuple2.of("b", 104), // h
                Tuple2.of("b", 101), // e
                Tuple2.of("b", 114), // r
                Tuple2.of("b", 101), // e
                Tuple2.of("b", 33)   // !
        );

        // Apply Flink operators
        DataStream<String> resultDataStream = dataStream
                .keyBy(tuple -> tuple.f0)
                // IMPORTANT: fold is deprecated
                .fold(
                        "hello ", // initial accumulator value
                        new SaluteFold()
                );

        // Print the DataStream
        resultDataStream.print();

        // Sink data
        //dataStream.addSink(...);

        // Execute program
        env.execute("Flink Streaming Job (Fold example)");
    }

    private static class SaluteFold implements FoldFunction<Tuple2<String, Integer>, String> {
        @Override
        public String fold(String accumulator, Tuple2<String, Integer> value) {
            // Transform the int value contained in the tuple to an ascii char, and add it to the accumulated String
            return accumulator + (char) value.f1.intValue();
        }
    }

}
