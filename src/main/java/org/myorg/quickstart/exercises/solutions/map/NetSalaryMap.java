package org.myorg.quickstart.exercises.solutions.map;

import org.apache.flink.api.common.functions.MapFunction;
import org.myorg.quickstart.exercises.model.Person;

public class NetSalaryMap implements MapFunction<Person, Person> {
    @Override
    public Person map(Person person) {
        person.setNetSalary(person.getGrossSalary() * 0.6);
        return person;
    }
}
