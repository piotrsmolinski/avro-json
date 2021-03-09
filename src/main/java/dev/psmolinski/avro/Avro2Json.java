package dev.psmolinski.avro;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericEnumSymbol;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;

import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Utility to convert a generic Avro object to the equivalent JSON representation.
 * The component solves the problem that the regular Avro library renders unions
 * using the same member selection mechanism as the one used internally in Avro.
 * This appears confusing for optional keys in the JSON hashes (maps).
 * The non-null optional values are rendered as hashes with a single entry
 * having key as the selected alternative type name.
 * The feature is available as
 * {@link org.apache.avro.io.EncoderFactory#jsonEncoder(Schema, OutputStream) jsonEncoder}.
 *
 * @author <a href="piotr.smolinski.77@gmail.com">Piotr Smolinski</a>
 */
public class Avro2Json {

    /**
     * Tuple of predicate and conversion function.
     * Instead of hardcoding the rules the selected approach uses list
     * of predicates and corresponding conversions.
     */
    private static class Rule {
        private final Predicate<Object> predicate;
        private final Function<Object, Object> function;
        public Rule(Predicate<Object> predicate, Function<Object, Object> function) {
            this.predicate = predicate;
            this.function = function;
        }
    }

    private final List<Rule> rules;

    public Avro2Json() {
        this.rules = defaultRules();
    }

    /**
     * Recursively convert the provided generic Avro compatible object to generic JSON.
     * The result is expressed as graph of {@link Map maps}, {@link List lists} and primitives,
     * compatible with Jackson's {@link com.fasterxml.jackson.databind.ObjectMapper}.
     *
     * @param o Avro object; accepted are {@link GenericRecord}, {@link GenericArray}, {@link Map}
     *          and primitives.
     * @return corresponding JSON object.
     */
    public Object convert(Object o) {
        // instead of hardcoding
        for (Rule rule : rules) {
            if (rule.predicate.test(o)) return rule.function.apply(o);
        }
        throw new IllegalArgumentException("Cannot convert object: "+o);
    }

    /**
     * The default rule definition is not static because the rules may call the converter
     * recursively.
     */
    private List<Rule> defaultRules() {
        List<Rule> rules = new ArrayList<>();
        rules.add(new Rule(this::isNull, this::convertNull));
        rules.add(new Rule(this::isRecord, this::convertRecord));
        rules.add(new Rule(this::isArray, this::convertArray));
        rules.add(new Rule(this::isMap, this::convertMap));
        rules.add(new Rule(this::isFixed, this::convertFixed));
        rules.add(new Rule(this::isBytes, this::convertBytes));
        rules.add(new Rule(this::isString, this::convertString));
        rules.add(new Rule(this::isEnumSymbol, this::convertEnumSymbol));
        rules.add(new Rule(this::isBoolean, this::convertBoolean));
        rules.add(new Rule(this::isInt, this::convertInt));
        rules.add(new Rule(this::isLong, this::convertLong));
        rules.add(new Rule(this::isFloat, this::convertFloat));
        rules.add(new Rule(this::isDouble, this::convertDouble));
        return rules;
    }

    // implementation of each predicate and conversion function follow

    private boolean isRecord(Object x) {
        return x instanceof GenericRecord;
    }
    private Object convertRecord(Object x) {
        GenericRecord record = (GenericRecord) x;
        Map<String, Object> map = new HashMap<>();
        for (Schema.Field f : record.getSchema().getFields()) {
            map.put(f.name(), convert(record.get(f.name())));
        }
        return map;
    }

    private boolean isMap(Object x) {
        return x instanceof Map;
    }
    private Object convertMap(Object x) {
        Map<String, Object> map = (Map<String,Object>) x;
        Map<String, Object> result = new HashMap<>();
        for (Map.Entry<String,Object> e : map.entrySet()) {
            result.put(e.getKey(), e.getValue());
        }
        return result;
    }

    private boolean isArray(Object x) {
        return x instanceof GenericArray;
    }
    private Object convertArray(Object x) {
        GenericArray array = (GenericArray) x;
        List<Object> list = new ArrayList<>(array.size());
        for (Object e : array) {
            list.add(convert(e));
        }
        return list;
    }

    private boolean isFixed(Object x) {
        return x instanceof GenericFixed;
    }
    private Object convertFixed(Object x) {
        GenericFixed fixed = (GenericFixed) x;
        return new String(fixed.bytes(), StandardCharsets.ISO_8859_1);
    }

    private boolean isBoolean(Object x) {
        return x instanceof Boolean;
    }
    private Object convertBoolean(Object x) {
        Boolean b = (Boolean) x;
        return b;
    }

    private boolean isInt(Object x) {
        return x instanceof Integer;
    }
    private Object convertInt(Object x) {
        Integer i = (Integer) x;
        return i;
    }

    private boolean isLong(Object x) {
        return x instanceof Long;
    }
    private Object convertLong(Object x) {
        Long l = (Long) x;
        return l;
    }

    private boolean isFloat(Object x) {
        return x instanceof Float;
    }
    private Object convertFloat(Object x) {
        Float f = (Float) x;
        return f;
    }

    private boolean isDouble(Object x) {
        return x instanceof Double;
    }
    private Object convertDouble(Object x) {
        Double f = (Double) x;
        return f;
    }

    private boolean isEnumSymbol(Object x) {
        return x instanceof GenericEnumSymbol;
    }
    private Object convertEnumSymbol(Object x) {
        GenericEnumSymbol enumSymbol = (GenericEnumSymbol) x;
        return enumSymbol.toString();
    }

    private boolean isBytes(Object x) {
        return x instanceof ByteBuffer;
    }
    private Object convertBytes(Object x) {
        ByteBuffer buffer = ((ByteBuffer) x).duplicate();
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        return new String(bytes, StandardCharsets.ISO_8859_1);
    }

    private boolean isString(Object x) {
        return x instanceof CharSequence;
    }
    private Object convertString(Object x) {
        CharSequence string = (CharSequence) x;
        return string.toString();
    }

    private boolean isNull(Object x) {
        return x == null;
    }
    private Object convertNull(Object x) {
        return null;
    }

}
