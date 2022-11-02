package org.myorg.quickstart.exercises.solutions.filter;

import org.apache.flink.api.common.functions.FilterFunction;
import org.myorg.quickstart.exercises.model.Person;

public class NetSalaryGreaterThanFilter implements FilterFunction<Person> {

    private final int salary;

    public NetSalaryGreaterThanFilter(int salary) {
        this.salary = salary;
    }

    @Override
    public boolean filter(Person person) {
        return person.getNetSalary() > this.salary;
    }
}
