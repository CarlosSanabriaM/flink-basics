package org.myorg.quickstart.map;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.myorg.quickstart.utils.StreamExecutionEnvironmentType;
import org.myorg.quickstart.utils.Utils;


public class StreamingJobRichMap {

    public static void main(String[] args) throws Exception {
        // Set up the streaming execution environment
        final StreamExecutionEnvironment env = Utils
                .getStreamExecutionEnvironment(StreamExecutionEnvironmentType.DEPENDS_ON_CONTEXT);

        // Create a DataStream that contains some tuples
        DataStream<Tuple2<String, Integer>> dataStream = env
                .fromElements(
                        Tuple2.of("a", 1),
                        Tuple2.of("a", 2),
                        Tuple2.of("a", 3),
                        Tuple2.of("a", 4),
                        Tuple2.of("b", 5),
                        Tuple2.of("b", 6),
                        Tuple2.of("b", 7),
                        Tuple2.of("b", 8),
                        Tuple2.of("c", 9)
                );

        // Apply Flink operators
        DataStream<Tuple2<String, Integer>> resultDataStream = dataStream
                .keyBy(tuple -> tuple.f0)
                .map(new SumByGroupsRichMap());

        // Print the DataStream
        resultDataStream.print();

        // Sink data
        //dataStream.addSink(...);

        // Execute program
        env.execute("Flink Streaming Job (RichMap example)");
    }

    // This same example (sum all elements by group) can be done easier with a reduce function.
    // Check it on StreamingJobReduce class.
    // It can be done even easier with sum. Check it on StreamingJobSum class.
    private static class SumByGroupsRichMap extends RichMapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>> {

        private ValueState<Integer> sumState; // keyed state

        @Override
        public void open(Configuration conf) {
            // Create / initialize the flink states
            ValueStateDescriptor<Integer> descriptor = new ValueStateDescriptor<>("sum state", Integer.class);
            sumState = getRuntimeContext().getState(descriptor);
        }

        @Override
        public Tuple2<String, Integer> map(Tuple2<String, Integer> value) throws Exception {
            Integer sumStateValue = sumState.value(); // Obtain value in state
            // If the state didn't have value, store the value of the current event
            if (sumStateValue == null)
                sumStateValue = value.f1;
                // If the state had value, update it adding the current value
            else
                sumStateValue += value.f1;

            // Update the state
            sumState.update(sumStateValue);

            // Return the current summed value for this key
            value.f1 = sumStateValue;
            return value;
        }
    }

}
