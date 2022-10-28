package org.myorg.quickstart.exercises.operators.richmap;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.myorg.quickstart.exercises.model.Person;

public class SumSalaryByCountry extends RichMapFunction<Person, Tuple2<String, Double>> {

}
