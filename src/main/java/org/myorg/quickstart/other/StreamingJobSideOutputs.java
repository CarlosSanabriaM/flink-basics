package org.myorg.quickstart.other;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.myorg.quickstart.utils.StreamExecutionEnvironmentType;
import org.myorg.quickstart.utils.Utils;


/**
 * Each event in the DataStream can be sent to one, more or none of the side outputs, depending on the desired logic.
 */
public class StreamingJobSideOutputs {

    public static void main(String[] args) throws Exception {
        // Set up the streaming execution environment
        final StreamExecutionEnvironment env = Utils
                .getStreamExecutionEnvironment(StreamExecutionEnvironmentType.DEPENDS_ON_CONTEXT);

        // Create a DataStream that contains some numbers
        DataStream<Integer> dataStream = env.fromElements(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

        // Apply Flink operators
        SingleOutputStreamOperator<Integer> resultDataStream = dataStream
                .process(new IntegerEventsToSideOutputsProcessFunction());

        // Obtain the side outputs from the previous DataStream
        DataStream<Integer> divisibleBy2DataStream = resultDataStream.getSideOutput(divisibleBy2Tag);
        DataStream<Integer> divisibleBy3DataStream = resultDataStream.getSideOutput(divisibleBy3Tag);
        DataStream<Integer> divisibleBy4DataStream = resultDataStream.getSideOutput(divisibleBy4Tag);

        // Print the DataStreams
        // The string in the print is a prefix to identify to which DataSteam corresponds the printed event
        divisibleBy2DataStream.print("divisibleBy2");
        divisibleBy3DataStream.print("divisibleBy3");
        divisibleBy4DataStream.print("divisibleBy4");

        // Sink data
        //dataStream.addSink(...);

        // Execute program
        env.execute("Flink Streaming Job (SideOutputs example)");
    }


    // Tags for the side outputs
    private static final OutputTag<Integer> divisibleBy2Tag = new OutputTag<Integer>("divisibleBy2") {
    };
    private static final OutputTag<Integer> divisibleBy3Tag = new OutputTag<Integer>("divisibleBy3") {
    };
    private static final OutputTag<Integer> divisibleBy4Tag = new OutputTag<Integer>("divisibleBy4") {
    };

    public static final class IntegerEventsToSideOutputsProcessFunction extends ProcessFunction<Integer, Integer> {
        @Override
        public void processElement(Integer element, Context ctx, Collector<Integer> out) {
            if (element % 2 == 0) {
                ctx.output(divisibleBy2Tag, element);
            }
            if (element % 3 == 0) {
                ctx.output(divisibleBy3Tag, element);
            }
            if (element % 4 == 0) {
                ctx.output(divisibleBy4Tag, element);
            }
            // If not, do nothing (omit the event)
        }
    }

}
