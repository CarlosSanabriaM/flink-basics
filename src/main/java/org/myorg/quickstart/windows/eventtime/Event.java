package org.myorg.quickstart.windows.eventtime;

import lombok.*;

// Java and Scala classes are treated by Flink as a special POJO data type if they fulfill the following requirements:
// * The class must be public.
// * It must have a public constructor without arguments (default constructor) --> @NoArgsConstructor
// * All fields are either public or must be accessible through getter and setter functions --> @Data creates getters
// * The type of a field must be supported by a registered serializer.
@SuppressWarnings("WeakerAccess")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Event {
    @NonNull
    private String key;
    @NonNull
    private Integer value;
    @NonNull
    private Long timestamp;
}
