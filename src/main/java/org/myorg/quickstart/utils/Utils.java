package org.myorg.quickstart.utils;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Utils {

    public static StreamExecutionEnvironment getStreamExecutionEnvironment(StreamExecutionEnvironmentType streamExecutionEnvironmentType) {
        StreamExecutionEnvironment env;

        switch (streamExecutionEnvironmentType) {
            case DEPENDS_ON_CONTEXT:
                // Creates an execution environment that represents the context in which the program is currently executed.
                // If the program is invoked standalone, this method returns a local execution environment, as returned by createLocalEnvironment().
                env = StreamExecutionEnvironment.getExecutionEnvironment();
                break;
            case LOCAL_WITH_WEB_UI:
                // Specific situation where the program is invoked standalone (you need a local execution environment)
                // and the web UI is required. The Flink Web UI is launched by default in http://localhost:8081.
                // The message "Web frontend listening at http://localhost:8081" must appear on the logs.
                System.out.println("Stream Execution Local Environment with web UI was selected.");
                System.out.println("Visit http://localhost:8081 to see the UI.");

                Configuration conf = new Configuration();
                env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
                break;
            default:
                throw new IllegalArgumentException();
        }

        // Set Event time
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        return env;
    }

}
