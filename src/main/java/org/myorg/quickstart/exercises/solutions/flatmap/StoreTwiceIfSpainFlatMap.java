package org.myorg.quickstart.exercises.solutions.flatmap;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.myorg.quickstart.exercises.model.Person;

public class StoreTwiceIfSpainFlatMap implements FlatMapFunction<Person, Person> {
    @Override
    public void flatMap(Person person, Collector<Person> out) {
        if (person.getCountry().equals("Spain")) {
            out.collect(person);
            out.collect(person);
        }
    }
}
