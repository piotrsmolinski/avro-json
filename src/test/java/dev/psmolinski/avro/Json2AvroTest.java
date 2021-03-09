package dev.psmolinski.avro;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericEnumSymbol;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.*;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.util.Utf8;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Map;

public class Json2AvroTest {

    private Schema schema;

    @BeforeEach
    public void beforeEach() throws Exception {
        schema = new Schema.Parser().parse(new FileInputStream("src/test/avro/Example.avsc"));
    }

    @Test
    public void testJsonToGenericAvro() throws Exception {

        Decoder decoder = DecoderFactory.get().jsonDecoder(schema, new FileInputStream("src/test/json/Example-avro.json") );
        DatumReader<Object> reader = GenericData.get().createDatumReader(schema);

        GenericRecord record = (GenericRecord) reader.read(null, decoder);

        Assertions.assertThat(record.get("id"))
                .isEqualTo("abcdefgh"); // id is extended string type
        Assertions.assertThat(record.get("description"))
                .isEqualTo(new Utf8("Sample")); // description has no annotation
        Assertions.assertThat(record.get("empty"))
                .isNull();
        Assertions.assertThat(record.get("direction"))
                .isInstanceOf(GenericEnumSymbol.class);
        Assertions.assertThat(((GenericEnumSymbol)record.get("direction")).toString())
                .isEqualTo("N");
        Assertions.assertThat(record.get("suit"))
                .isInstanceOf(GenericEnumSymbol.class);
        Assertions.assertThat(((GenericEnumSymbol)record.get("suit")).toString())
                .isEqualTo("SPADES");
        Assertions.assertThat(record.get("age"))
                .isEqualTo(28);
        Assertions.assertThat(record.get("balance"))
                .isEqualTo(14563.27);
        Assertions.assertThat(record.get("code"))
                .isEqualTo(ByteBuffer.wrap(new byte[]{(byte)0x00, (byte)0x05, (byte)0x45, (byte)0x8a, (byte)0xf3}));
        Assertions.assertThat(record.get("digest"))
                .isInstanceOf(GenericFixed.class);
        Assertions.assertThat(((GenericFixed)record.get("digest")).bytes())
                .isEqualTo(MessageDigest.getInstance("MD5").digest("testtesttest".getBytes()));
        Assertions.assertThat(record.get("test"))
                .isEqualTo(2);

    }

    @Test
    public void testJsonToSpecificAvro() throws Exception {

        Decoder decoder = DecoderFactory.get().jsonDecoder(schema, new FileInputStream("src/test/json/Example-avro.json") );
        DatumReader<Object> reader = SpecificData.get().createDatumReader(schema);

        Example record = (Example) reader.read(null, decoder);

        Assertions.assertThat(record.getId())
                .isEqualTo("abcdefgh"); // binding is generated with String
        Assertions.assertThat(record.getDescription())
                .isEqualTo("Sample"); // binding is generated with String
        Assertions.assertThat(record.getEmpty())
                .isNull();
        Assertions.assertThat(record.getDirection())
                .isEqualTo(Direction.N);
        Assertions.assertThat(record.getSuit())
                .isEqualTo(Suit.SPADES);
        Assertions.assertThat(record.getAge())
                .isEqualTo(28);
        Assertions.assertThat(record.getBalance())
                .isEqualTo(14563.27);
        Assertions.assertThat(record.getCode())
                .isEqualTo(ByteBuffer.wrap(new byte[]{(byte)0x00, (byte)0x05, (byte)0x45, (byte)0x8a, (byte)0xf3}));
        Assertions.assertThat(record.getDigest().bytes())
                .isEqualTo(MessageDigest.getInstance("MD5").digest("testtesttest".getBytes()));
        Assertions.assertThat(record.getTest())
                .isEqualTo(2);

    }

    @Test
    public void testJsonToAvro() throws Exception {

        ObjectMapper om = new ObjectMapper();
        Map<String, Object> json = (Map<String,Object>)om.readValue(new FileInputStream("src/test/json/Example-json.json"), Object.class);

        Object avro = new Json2Avro().convert(json, schema);

        GenericRecord record = (GenericRecord) fromBytes(toBytes(avro, schema), schema);

        Assertions.assertThat(record.get("id"))
                .isEqualTo("abcdefgh"); // id is extended string type
        Assertions.assertThat(record.get("description"))
                .isEqualTo(new Utf8("Sample")); // description has no annotation
        Assertions.assertThat(record.get("empty"))
                .isNull();
        Assertions.assertThat(record.get("direction"))
                .isInstanceOf(GenericEnumSymbol.class);
        Assertions.assertThat(((GenericEnumSymbol)record.get("direction")).toString())
                .isEqualTo("N");
        Assertions.assertThat(record.get("suit"))
                .isInstanceOf(GenericEnumSymbol.class);
        Assertions.assertThat(((GenericEnumSymbol)record.get("suit")).toString())
                .isEqualTo("SPADES");
        Assertions.assertThat(record.get("age"))
                .isEqualTo(28);
        Assertions.assertThat(record.get("balance"))
                .isEqualTo(14563.27);
        Assertions.assertThat(record.get("code"))
                .isEqualTo(ByteBuffer.wrap(new byte[]{(byte)0x00, (byte)0x05, (byte)0x45, (byte)0x8a, (byte)0xf3}));
        Assertions.assertThat(record.get("digest"))
                .isInstanceOf(GenericFixed.class);
        Assertions.assertThat(((GenericFixed)record.get("digest")).bytes())
                .isEqualTo(MessageDigest.getInstance("MD5").digest("testtesttest".getBytes()));
        Assertions.assertThat(record.get("test"))
                .isEqualTo(2);

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
