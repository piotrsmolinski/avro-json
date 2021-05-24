package dev.psmolinski.avro;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

public class Avro2JsonTest {

    private Schema schema;

    @BeforeEach
    public void beforeEach() throws Exception {
        schema = new Schema.Parser().parse(new FileInputStream("src/test/avro/Example.avsc"));
    }

    @Test
    public void testGenericAvroToJson() throws Exception {

        GenericRecord record = new GenericData.Record(schema);

        record.put("id", "abcdefgh");
        record.put("description", "Sample");
        record.put("direction",
                new GenericData.EnumSymbol(schema.getField("direction").schema().getTypes().get(1), "N"));
        record.put("suit",
                new GenericData.EnumSymbol(schema.getField("suit").schema(), "SPADES"));
        record.put("age", 28);
        record.put("balance", 14563.27);
        record.put("code", ByteBuffer.wrap(new byte[]{(byte)0x00, (byte)0x05, (byte)0x45, (byte)0x8a, (byte)0xf3}));
        record.put("digest", new GenericData.Fixed(schema.getField("digest").schema(),
                MessageDigest.getInstance("MD5").digest("testtesttest".getBytes())
        ));
        record.put("test", 2);
        record.put("taxRate", 10.2);
        record.put("active", true);
        record.put("timestamp", 1597792595230L);
        record.put("date", new GenericData.Array<>(schema.getField("date").schema(), Arrays
            .asList(2021, 1, 31)));
        record.put("tokens", Collections.singletonMap("username1", "password1"));

        // This creates a clone of the original record by serialization and deserialization.
        // The reason is that we want to be sure the generic Avro object is fully validated.
        Object avro = fromBytes(toBytes(record, schema), schema);

        Object json = new Avro2Json().convert(avro);

        ObjectMapper om = new ObjectMapper();
        om.enable(SerializationFeature.INDENT_OUTPUT);

        Map<String, Object> map = (Map<String,Object>)om.readValue(om.writeValueAsString(json), Object.class);

        Assertions.assertThat(map.get("id"))
                .isEqualTo("abcdefgh");
        Assertions.assertThat(map.get("description"))
                .isEqualTo("Sample");
        Assertions.assertThat(map.get("empty"))
                .isNull();
        Assertions.assertThat(map.get("direction"))
                .isEqualTo("N");
        Assertions.assertThat(map.get("suit"))
                .isEqualTo("SPADES");
        Assertions.assertThat(map.get("age"))
                .isEqualTo(28);
        Assertions.assertThat(map.get("balance"))
                .isEqualTo(14563.27);
        Assertions.assertThat(map.get("code"))
                .isEqualTo(new String(new byte[]{(byte)0x00, (byte)0x05, (byte)0x45, (byte)0x8a, (byte)0xf3}, StandardCharsets.ISO_8859_1));
        Assertions.assertThat(map.get("digest"))
                .isEqualTo(new String(MessageDigest.getInstance("MD5").digest("testtesttest".getBytes()), StandardCharsets.ISO_8859_1));
        Assertions.assertThat(map.get("test"))
                .isEqualTo(2);
        Assertions.assertThat(map.get("taxRate"))
                .isEqualTo(10.2);
        Assertions.assertThat(map.get("active"))
                .isEqualTo(true);
        Assertions.assertThat(map.get("timestamp"))
                .isEqualTo(1597792595230L);
        Assertions.assertThat(map.get("date"))
                .isEqualTo(Arrays.asList(2021, 1, 31));
        Assertions.assertThat(map.get("tokens"))
                .isEqualTo(Collections.singletonMap("username1", "password1"));

    }

    @Test
    public void testSpecificAvroToJson() throws Exception {

        Example record = Example.newBuilder()
                .setId("abcdefgh")
                .setDescription("Sample")
                .setEmpty(null)
                .setDirection(Direction.N)
                .setSuit(Suit.SPADES)
                .setAge(28)
                .setBalance(14563.27)
                .setCode(ByteBuffer.wrap(new byte[]{(byte)0x00, (byte)0x05, (byte)0x45, (byte)0x8a, (byte)0xf3}))
                .setDigest(new MD5(MessageDigest.getInstance("MD5").digest("testtesttest".getBytes())))
                .setTest(2)
                .setTaxRate(10.2f)
                .setActive(true)
                .setTimestamp(1597792595230L)
                .setDate(Arrays.asList(2021, 1, 31))
                .setTokens(Collections.singletonMap("username1", "password1"))
                .build();

        // This converts the specific Avro record to a generic one.
        // This time it is necessary because the converter does not understand Java binding classes.
        Object avro = fromBytes(toBytes(record, schema), schema);

        Object json = new Avro2Json().convert(avro);

        ObjectMapper om = new ObjectMapper();
        om.enable(SerializationFeature.INDENT_OUTPUT);

        Map<String, Object> map = (Map<String,Object>)om.readValue(om.writeValueAsString(json), Object.class);

        Assertions.assertThat(map.get("id"))
                .isEqualTo("abcdefgh");
        Assertions.assertThat(map.get("description"))
                .isEqualTo("Sample");
        Assertions.assertThat(map.get("empty"))
                .isNull();
        Assertions.assertThat(map.get("direction"))
                .isEqualTo("N");
        Assertions.assertThat(map.get("suit"))
                .isEqualTo("SPADES");
        Assertions.assertThat(map.get("age"))
                .isEqualTo(28);
        Assertions.assertThat(map.get("balance"))
                .isEqualTo(14563.27);
        Assertions.assertThat(map.get("code"))
                .isEqualTo(new String(new byte[]{(byte)0x00, (byte)0x05, (byte)0x45, (byte)0x8a, (byte)0xf3}, StandardCharsets.ISO_8859_1));
        Assertions.assertThat(map.get("digest"))
                .isEqualTo(new String(MessageDigest.getInstance("MD5").digest("testtesttest".getBytes()), StandardCharsets.ISO_8859_1));
        Assertions.assertThat(map.get("test"))
                .isEqualTo(2);
        Assertions.assertThat(map.get("taxRate"))
            .isEqualTo(10.2);
        Assertions.assertThat(map.get("active"))
            .isEqualTo(true);
        Assertions.assertThat(map.get("timestamp"))
            .isEqualTo(1597792595230L);
        Assertions.assertThat(map.get("date"))
            .isEqualTo(Arrays.asList(2021, 1, 31));
        Assertions.assertThat(map.get("tokens"))
            .isEqualTo(Collections.singletonMap("username1", "password1"));

    }

    @Test
    public void testNativeAvroToJson() throws Exception {

        GenericRecord record = new GenericData.Record(schema);

        record.put("id", "abcdefgh");
        record.put("description", "Sample");
        record.put("direction",
                new GenericData.EnumSymbol(schema.getField("direction").schema().getTypes().get(1), "N"));
        record.put("suit",
                new GenericData.EnumSymbol(schema.getField("suit").schema(), "SPADES"));
        record.put("age", 28);
        record.put("balance", 14563.27);
        record.put("code", ByteBuffer.wrap(new byte[]{(byte)0x00, (byte)0x05, (byte)0x45, (byte)0x8a, (byte)0xf3}));
        record.put("digest", new GenericData.Fixed(schema.getField("digest").schema(),
                MessageDigest.getInstance("MD5").digest("testtesttest".getBytes())
        ));
        record.put("test", 2);
        record.put("taxRate", 10.2);
        record.put("active", true);
        record.put("timestamp", 1597792595230L);
        record.put("date", new GenericData.Array<>(schema.getField("date").schema(), Arrays
            .asList(2021, 1, 31)));
        record.put("tokens", Collections.singletonMap("username1", "password1"));

        Object avro = fromBytes(toBytes(record, schema), schema);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        Encoder encoder = EncoderFactory.get().jsonEncoder(schema, baos, true);
        DatumWriter<Object> writer = GenericData.get().createDatumWriter(schema);
        writer.write(avro, encoder);
        encoder.flush();

        String json = new String(baos.toByteArray(), StandardCharsets.UTF_8);

        ObjectMapper om = new ObjectMapper();
        om.enable(SerializationFeature.INDENT_OUTPUT);

        Map<String, Object> map = (Map<String,Object>)om.readValue(json, Object.class);

        Assertions.assertThat(map.get("id"))
                .isEqualTo("abcdefgh");
        Assertions.assertThat(map.get("description"))
                .isInstanceOf(Map.class);
        Assertions.assertThat(((Map<String,Object>)map.get("description")).get("string"))
                .isEqualTo("Sample");
        Assertions.assertThat(map.get("empty"))
                .isNull();
        Assertions.assertThat(map.get("direction"))
                .isInstanceOf(Map.class);
        Assertions.assertThat(((Map<String,Object>)map.get("direction")).get("dev.psmolinski.avro.Direction"))
                .isEqualTo("N");
        Assertions.assertThat(map.get("suit"))
                .isEqualTo("SPADES");
        Assertions.assertThat(map.get("age"))
                .isEqualTo(28);
        Assertions.assertThat(map.get("balance"))
                .isEqualTo(14563.27);
        Assertions.assertThat(map.get("code"))
                .isEqualTo(new String(new byte[]{(byte)0x00, (byte)0x05, (byte)0x45, (byte)0x8a, (byte)0xf3}, StandardCharsets.ISO_8859_1));
        Assertions.assertThat(map.get("digest"))
                .isEqualTo(new String(MessageDigest.getInstance("MD5").digest("testtesttest".getBytes()), StandardCharsets.ISO_8859_1));
        Assertions.assertThat(map.get("test"))
                .isInstanceOf(Map.class);
        Assertions.assertThat(((Map<String,Object>)map.get("test")).get("int"))
                .isEqualTo(2);
        Assertions.assertThat(map.get("taxRate"))
            .isEqualTo(10.2);
        Assertions.assertThat(map.get("active"))
            .isEqualTo(true);
        Assertions.assertThat(map.get("timestamp"))
            .isEqualTo(1597792595230L);
        Assertions.assertThat(map.get("date"))
            .isEqualTo(Arrays.asList(2021, 1, 31));
        Assertions.assertThat(map.get("tokens"))
            .isEqualTo(Collections.singletonMap("username1", "password1"));

    }

    private <T> byte[] toBytes(T avro, Schema schema) throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        Encoder encoder = EncoderFactory.get().binaryEncoder(baos, null);
        DatumWriter<T> writer = GenericData.get().createDatumWriter(schema);
        writer.write(avro, encoder);
        encoder.flush();
        return baos.toByteArray();
    }

    private <T> T fromBytes(byte[] bytes, Schema schema) throws Exception {
        Decoder decoder = DecoderFactory.get().binaryDecoder(bytes, null);
        DatumReader<T> reader = GenericData.get().createDatumReader(schema);
        return reader.read(null, decoder);
    }

}
