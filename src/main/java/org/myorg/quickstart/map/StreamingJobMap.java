package org.myorg.quickstart.map;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.myorg.quickstart.utils.StreamExecutionEnvironmentType;
import org.myorg.quickstart.utils.Utils;


public class StreamingJobMap {

    public static void main(String[] args) throws Exception {
        // Set up the streaming execution environment
        final StreamExecutionEnvironment env = Utils
                .getStreamExecutionEnvironment(StreamExecutionEnvironmentType.DEPENDS_ON_CONTEXT);

        // Create a DataStream that contains some numbers
        DataStream<Integer> dataStream = env.fromElements(1, 2, 3, 4, 5);

        // Apply Flink operators
        DataStream<Integer> resultDataStream = dataStream
                .map(new MultiplyBy2Map());

        // Print the DataStream
        resultDataStream.print();

        // Sink data
        //dataStream.addSink(...);

        // Execute program
        env.execute("Flink Streaming Job (Map example)");
    }

    private static class MultiplyBy2Map implements MapFunction<Integer, Integer> {
        @Override
        public Integer map(Integer value) {
            return 2 * value;
        }
    }

}
