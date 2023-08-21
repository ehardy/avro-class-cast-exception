package example;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.SchemaProvider;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import org.apache.avro.Schema;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Optional;

public class StringListExampleTest {

    private final SchemaProvider schemaProvider = new AvroSchemaProvider();

    //The generated object's SCHEMA$ property specifies the avro.java.string property even if the schema definition
    //in the .avsc file does not mention them, so this test succeeds
    @Test
    void stringListSerializeAndDeserializeUsingClassSchema() {
        ExampleObject1 object = new ExampleObject1();

        object.setSimpleString("test");
        object.setStringList(Arrays.asList("one", "two"));

        ExampleObject1 result = encodeAndDecode(object, ExampleObject1.SCHEMA$);

        System.out.println("got object, stringList.length = " + result.getStringList());
        System.out.println("type of stringList objects = " + result.getStringList().get(0).getClass());
    }

    //This test fails with a ClassCastException, objects found with stringList are of type org.apache.avro.util.Utf8
    @Test
    void stringListSerializeAndDeserializeUsingSchemaFileNoAvroJavaString() throws Exception {
        ParsedSchema schema = readSchema("/avro/Example1.avsc");

        ExampleObject1 object = new ExampleObject1();

        object.setSimpleString("test");
        object.setStringList(Arrays.asList("one", "two"));

        ExampleObject1 result = encodeAndDecode(object, (Schema)schema.rawSchema());

        System.out.println("got object, stringList.length = " + result.getStringList());
        System.out.println("type of stringList objects = " + result.getStringList().get(0).getClass());
    }

    @Test
    void stringListSerializeAndDeserializeUsingSchemaFileWithAvroJavaString() throws Exception {
        ParsedSchema schema = readSchema("/avro/Example2.avsc");

        ExampleObject2 object = new ExampleObject2();

        object.setSimpleString("test");
        object.setStringList(Arrays.asList("one", "two"));

        ExampleObject2 result = encodeAndDecode(object, (Schema)schema.rawSchema());

        System.out.println("got object, stringList.length = " + result.getStringList());
        System.out.println("type of stringList objects = " + result.getStringList().get(0).getClass());
    }

    private ParsedSchema readSchema(String schemaResource) throws IOException {
        try (InputStream input = getClass().getResourceAsStream(schemaResource)) {
            byte[] bytes = input.readAllBytes();
            String schemaString = new String(bytes, StandardCharsets.UTF_8);

            Optional<ParsedSchema> schema = schemaProvider.parseSchema(schemaString, null);

            return schema.orElseThrow();
        }
    }

    private <T> T encodeAndDecode(T object, Schema schema) {
        DatumWriter<T> writer = new SpecificDatumWriter<>(schema);
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        Encoder encoder = EncoderFactory.get().directBinaryEncoder(stream, null);
        byte[] encoded;

        try {
            writer.write(object, encoder);
            encoder.flush();

            encoded = stream.toByteArray();

            System.out.println("encoded.length = " + encoded.length);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        DatumReader<T> reader = new SpecificDatumReader<>(schema);
        Decoder decoder = DecoderFactory.get().directBinaryDecoder(new ByteArrayInputStream(encoded), null);

        try {
            return reader.read(null, decoder);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
