package org.myorg.quickstart.exercises.model;

import lombok.*;

// Required annotations to have:
// * Builder class
// * NoArgsConstructor required by Flink to be considered a POJO
@With
@Builder
@AllArgsConstructor(access = AccessLevel.PACKAGE) // without this annotation, Builder + NoArgsConstructor doesn't work
@NoArgsConstructor
@Data
public class Person {
    private String name;
    private int age;
    private double grossSalary;
    private double netSalary;
    private String country;
}
