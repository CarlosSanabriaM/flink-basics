package org.myorg.quickstart.exercises.solutions.richmap;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.myorg.quickstart.exercises.model.Person;

public class SumSalaryByCountry extends RichMapFunction<Person, Tuple2<String, Double>> {

    private transient ValueState<Double> sumState;

    @Override
    public void open(Configuration parameters) {
        sumState = getRuntimeContext().getState(new ValueStateDescriptor<>("sum", Types.DOUBLE));
    }

    @Override
    public Tuple2<String, Double> map(Person person) throws Exception {
        Double sum = sumState.value();
        if (sum == null)
            sum = 0.0;

        sum += person.getGrossSalary();
        sumState.update(sum);

        return Tuple2.of(person.getCountry(), sum);
    }

}
