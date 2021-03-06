package org.myorg.quickstart.filter;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.myorg.quickstart.utils.StreamExecutionEnvironmentType;
import org.myorg.quickstart.utils.Utils;


public class StreamingJobFilter {

    public static void main(String[] args) throws Exception {
        // Set up the streaming execution environment
        final StreamExecutionEnvironment env = Utils
                .getStreamExecutionEnvironment(StreamExecutionEnvironmentType.DEPENDS_ON_CONTEXT);

        // Create a DataStream that contains some numbers
        DataStream<Integer> dataStream = env.fromElements(1, 2, 3, 4, 5);

        // Apply Flink operators
        DataStream<Integer> resultDataStream = dataStream
                .filter(new OddFilter());

        // Print the DataStream
        resultDataStream.print();

        // Sink data
        //dataStream.addSink(...);

        // Execute program
        env.execute("Flink Streaming Job (Filter example)");
    }

    private static class OddFilter implements FilterFunction<Integer> {
        @Override
        public boolean filter(Integer value) {
            return value % 2 == 0;
        }
    }

}
