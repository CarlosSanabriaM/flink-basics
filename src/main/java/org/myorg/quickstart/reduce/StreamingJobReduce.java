package org.myorg.quickstart.reduce;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.myorg.quickstart.utils.StreamExecutionEnvironmentType;
import org.myorg.quickstart.utils.Utils;


public class StreamingJobReduce {

    public static void main(String[] args) throws Exception {
        // Set up the streaming execution environment
        final StreamExecutionEnvironment env = Utils
                .getStreamExecutionEnvironment(StreamExecutionEnvironmentType.DEPENDS_ON_CONTEXT);

        // Create a DataStream that contains some tuples
        DataStream<Tuple2<String, Integer>> dataStream = env.fromElements(
                Tuple2.of("a", 1),
                Tuple2.of("a", 2),
                Tuple2.of("a", 3),
                Tuple2.of("a", 4),
                Tuple2.of("b", 5),
                Tuple2.of("b", 6),
                Tuple2.of("b", 7),
                Tuple2.of("b", 8),
                Tuple2.of("c", 9) // With this key, the reduce function is not invoked, because there is only 1 event whose key is c
        );

        // Apply Flink operators
        DataStream<Tuple2<String, Integer>> resultDataStream = dataStream
                .keyBy(tuple -> tuple.f0)
                .reduce(new SumByGroupsReduce());

        // Print the DataStream
        resultDataStream.print();

        // Sink data
        //dataStream.addSink(...);

        // Execute program
        env.execute("Flink Streaming Job (Reduce example)");
    }

    private static class SumByGroupsReduce implements ReduceFunction<Tuple2<String, Integer>> {
        @Override
        public Tuple2<String, Integer> reduce(Tuple2<String, Integer> tuple1, Tuple2<String, Integer> tuple2) {
            // This function is not invoked if only one event of a key arrives
            // In the first invocation to this method, tuple1 will have the value of the first event
            // In the next invocations, tuple1 will have the accumulated value.

            System.out.println("key: " + tuple1.f0 + " -> tuple1: " + tuple1 + " tuple2: " + tuple2);

            tuple1.f1 = tuple1.f1 + tuple2.f1;
            return tuple1;
        }
    }

}
