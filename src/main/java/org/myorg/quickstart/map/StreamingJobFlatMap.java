package org.myorg.quickstart.map;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.myorg.quickstart.utils.StreamExecutionEnvironmentType;
import org.myorg.quickstart.utils.Utils;


public class StreamingJobFlatMap {

    public static void main(String[] args) throws Exception {
        // Set up the streaming execution environment
        final StreamExecutionEnvironment env = Utils
                .getStreamExecutionEnvironment(StreamExecutionEnvironmentType.DEPENDS_ON_CONTEXT);

        // Create a DataStream that contains some numbers
        DataStream<Integer> dataStream = env.fromElements(1, 2, 3, 4, 5);

        // Apply Flink operators
        DataStream<Integer> resultDataStream = dataStream
                .flatMap(new StoreTwiceIfEvenFlatMap());

        // Print the DataStream
        resultDataStream.print();

        // Sink data
        //dataStream.addSink(...);

        // Execute program
        env.execute("Flink Streaming Job (FlatMap example)");
    }

    private static class StoreTwiceIfEvenFlatMap implements FlatMapFunction<Integer, Integer> {
        @Override
        public void flatMap(Integer value, Collector<Integer> out) {
            if (value % 2 == 0) {
                out.collect(value);
                out.collect(value);
            }
        }
    }

}
