package org.myorg.quickstart.exercises.main;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.myorg.quickstart.exercises.operators.filter.NetSalaryGreaterThanFilter;
import org.myorg.quickstart.exercises.operators.flatmap.StoreTwiceIfSpainFlatMap;
import org.myorg.quickstart.exercises.operators.map.NetSalaryMap;
import org.myorg.quickstart.exercises.operators.richmap.SumSalaryByCountry;
import org.myorg.quickstart.exercises.model.Person;

import java.util.ArrayList;
import java.util.Random;

/**
 * <h1>Ejercicios a realizar</h1>
 * <ol>
 * <li>Calcular el salario neto de cada persona. Para simplificar, el salario neto será el 60% del salario bruto.</li>
 * <li>Aplicar un filtro al resultado del ejercicio anterior: solo nos quedamos con aquellas personas que tengan un salario neto mayor a 25,000.</li>
 * <li>Sacar el sumatorio de sueldo bruto por país. Esto es, el resultado será un DataStream de tuplas de dos elementos,
 * donde la clave sea el país y el valor sea la suma de todos los sueldos brutos de las personas pertenecientes a dicho país
 * (String - país, Double - suma total).</li>
 * <li>Aplicar una función sobre el primer DataStream de todos donde se escriban los usuarios españoles dos veces.</li>
 * </ol>
 */
public class Main {
    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(10);

        // Get data
        ArrayList<Person> people = createPeople(10);

        // Get initial DataStream
        DataStream<Person> dsPeople = env.fromCollection(people);
        dsPeople.print("dsPeople");

        // Get net salary
        DataStream<Person> dsNetSalary = dsPeople.map(new NetSalaryMap());
        dsNetSalary.print("dsNetSalary");

        // People with a net salary greater than 25,000,000
        DataStream<Person> dsNetSalaryGreaterThan33000 = dsNetSalary.filter(new NetSalaryGreaterThanFilter(25000));
        dsNetSalaryGreaterThan33000.print("dsNetSalaryGreaterThan33000");

        // Sum of salary by country
        DataStream<Tuple2<String, Double>> dsSalaryByCountry = dsPeople
                .keyBy(Person::getCountry)
                .map(new SumSalaryByCountry());
        dsSalaryByCountry.print("dsSalaryByCountry");

        // Sink twice people from Spain
        DataStream<Person> dsTwiceSpanish = dsPeople
                .keyBy(Person::getCountry)
                .flatMap(new StoreTwiceIfSpainFlatMap());
        dsTwiceSpanish.print("dsTwiceSpanish");

        env.execute();
    }


    private static ArrayList<Person> createPeople(int numberOfPeople) {
        String[] countries = {"Spain", "Italy", "Germany", "England"};
        int[] salaries = {20000, 25000, 30000, 35000, 40000, 50000, 65000, 80000, 100000};

        ArrayList<Person> people = new ArrayList<>();

        for (int i = 0; i < numberOfPeople; i++) {
            Person person = Person.builder()
                    .age(0)
                    .country(countries[new Random().nextInt(countries.length)])
                    .grossSalary(salaries[new Random().nextInt(salaries.length)])
                    .name("name-" + i)
                    .build();

            people.add(person);
        }
        return people;
    }
}
