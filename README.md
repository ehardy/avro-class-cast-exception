# avro-class-cast-exception

Just run `mvn clean test` to reproduce the problem. The test case reproducing the problem is found 
in `StringListExampleTest`. Avro schemas are found in `src/main/resources/avro`. There are two
schemas, `Example1.avsc` and `Example2.avsc`. They both have the same properties, the only difference
being that `Example2.avsc` specifies the `avro.java.string` property on types.