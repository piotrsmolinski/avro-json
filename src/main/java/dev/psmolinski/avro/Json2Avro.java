package dev.psmolinski.avro;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericEnumSymbol;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;

import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Predicate;

/**
 * Utility to convert a parsed JSON content, expressed as generic graph,
 * into generic Avro model matching the provided schema.
 * The default JSON parsing in Avro expects all UNION to be hashes with a single
 * key specifying which alternative should be used.
 * This component implements an approach where there is an attempt to cast the content
 * to each of the alternative; the first one to succeed wins.
 * JSON implementation in Avro has the advantage that the transformation to JSON
 * is fully reversible.
 * The implementation of this component may render some distinct corner cases to the same
 * JSON representation.
 * <br/>
 * The default JSON parsing in Avro is available as
 * {@link org.apache.avro.io.DecoderFactory#jsonDecoder(Schema, InputStream) jsonDecoder}
 *
 * @author <a href="piotr.smolinski.77@gmail.com">Piotr Smolinski</a>
 */
public class Json2Avro {

    /**
     * Dedicated exception to capture the conversion failure. The primary use is
     * to detect to which Avro union member the JSON content should be cast.
     * Outside of this component it has semantics of {@link IllegalArgumentException}.
     */
    private static class ConversionUnsupportedException extends IllegalArgumentException {
        public ConversionUnsupportedException(String message) {
            super(message);
        }
    }

    /**
     * Tuple of predicate and conversion function.
     * Instead of hardcoding the rules the selected approach uses list
     * of predicates and corresponding casting.
     */
    private static class Rule {
        private final Predicate<Schema> predicate;
        private final BiFunction<Object, Schema, Object> function;
        public Rule(Predicate<Schema> predicate, BiFunction<Object, Schema, Object> function) {
            this.predicate = predicate;
            this.function = function;
        }
    }

    private final List<Rule> rules;

    public Json2Avro() {
        this.rules = defaultRules();
    }

    /**
     * Recursively cast the provided JSON content to the generic Avro object compatible
     * with the schema.
     * The result object could be {@link GenericRecord}, {@link GenericArray}, {@link GenericFixed},
     * {@link GenericEnumSymbol}, {@link Map} or any Avro primitive (including {@code null}).
     * In case of UNION the first conversion that does not fail is considered the correct one.
     * @param json JSON object ({@link Map}, {@link List}, any primitive, {@code null}
     * @return corresponding generic Avro object
     */
    public Object convert(Object json, Schema schema) {
        for (Rule rule : rules) {
            if (rule.predicate.test(schema)) return rule.function.apply(json, schema);
        }
        throw new IllegalArgumentException("No rule to convert schema: "+schema);
    }

    /**
     * The default rule definition is not static because the rules may call the converter
     * recursively.
     */
    private List<Rule> defaultRules() {
        List<Rule> rules = new ArrayList<>();
        rules.add(new Rule(this::isNull, this::convertNull));
        rules.add(new Rule(this::isUnion, this::convertUnion));
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

    private boolean isRecord(Schema schema) {
        return schema.getType() == Schema.Type.RECORD;
    }

    /**
     * Record is the first of the non-trivial conversions in this component.
     * The cast succeeds if:
     * <ul>
     *     <li>all keys in the hash have a field in the schema</li>
     *     <li>all mandatory fields are present in the hash</li>
     *     <li>all values are castable to the target field schemas</li>
     * </ul>
     * @param x JSON hash
     * @param schema target schema of type {@link Schema.Type#RECORD RECORD}
     * @return Avro record
     */
    private Object convertRecord(Object x, Schema schema) {
        if (x instanceof Map) {
            Map<String, Object> map = (Map<String, Object>) x;
            GenericRecord record = new GenericData.Record(schema);
            for (Map.Entry<String, Object> e : map.entrySet()) {
                Schema.Field f = schema.getField(e.getKey());
                if (f==null) {
                    throw new ConversionUnsupportedException("Unexpected field: "+e.getKey());
                }
                record.put(e.getKey(), convert(e.getValue(), f.schema()));
            }
            for (Schema.Field f : schema.getFields()) {
                if (map.containsKey(f.name())) continue;
                record.put(f.name(), convert(null, f.schema()));
            }
            return record;
        } else {
            throw new ConversionUnsupportedException("Expected Map<String,?> for RECORD");
        }
    }

    private boolean isMap(Schema schema) {
        return schema.getType() == Schema.Type.MAP;
    }
    private Object convertMap(Object x, Schema schema) {
        if (x instanceof Map) {
            Map<String, Object> map = (Map<String, Object>) x;
            Map<String, Object> result = new HashMap<>();
            for (Map.Entry<String, Object> e : map.entrySet()) {
                result.put(e.getKey(), convert(e.getValue(), schema.getValueType()));
            }
            return result;
        } else {
            throw new ConversionUnsupportedException("Expected Map<String,?> for MAP");
        }
    }

    private boolean isUnion(Schema schema) {
        return schema.getType() == Schema.Type.UNION;
    }

    /**
     * Union is the second of the non-trivial conversions in this component.
     * The cast succeeds if the content could be cast to any of the underlying schemas.
     *
     * @param x JSON object
     * @param schema target schema of type {@link Schema.Type#UNION UNION}
     * @return Avro object
     */
    private Object convertUnion(Object x, Schema schema) {
        for (Schema s : schema.getTypes()) try {
            return convert(x, s);
        } catch (ConversionUnsupportedException e) {
            // ignore, next
        }
        throw new ConversionUnsupportedException("No alternative matches for UNION");
    }

    private boolean isArray(Schema schema) {
        return schema.getType() == Schema.Type.ARRAY;
    }
    private Object convertArray(Object x, Schema schema) {
        if (x instanceof List) {
            List<Object> list = (List<Object>) x;
            List<Object> array = new ArrayList<>(list.size());
            for (Object e : list) {
                array.add(convert(e, schema.getElementType()));
            }
            return new GenericData.Array<>(schema, array);
        } else {
            throw new ConversionUnsupportedException("Expected List<?> for ARRAY");
        }
    }

    private boolean isFixed(Schema schema) {
        return schema.getType() == Schema.Type.FIXED;
    }
    private Object convertFixed(Object x, Schema schema) {
        if (x instanceof String) {
            String string = (String) x;
            return new GenericData.Fixed(schema, string.getBytes(StandardCharsets.ISO_8859_1));
        } else {
            throw new ConversionUnsupportedException("Expected String for FIXED");
        }
    }

    private boolean isBoolean(Schema schema) {
        return schema.getType() == Schema.Type.BOOLEAN;
    }
    private Object convertBoolean(Object x, Schema schema) {
        if (x instanceof Boolean) {
            return (Boolean)x;
        } else if (x instanceof Number) {
            Number n = (Number) x;
            return n.intValue() == 1;
        } else if (x instanceof String) {
            String string = (String) x;
            return Boolean.valueOf(string);
        } else {
            throw new ConversionUnsupportedException("Expected Boolean for BOOLEAN");
        }
    }

    private boolean isInt(Schema schema) {
        return schema.getType() == Schema.Type.INT;
    }
    private Object convertInt(Object x, Schema schema) {
        if (x instanceof Number) {
            Number n = (Number) x;
            return n.intValue();
        } else {
            throw new ConversionUnsupportedException("Expected Number for INT");
        }
    }

    private boolean isLong(Schema schema) {
        return schema.getType() == Schema.Type.LONG;
    }
    private Object convertLong(Object x, Schema schema) {
        if (x instanceof Number) {
            Number n = (Number) x;
            return n.longValue();
        } else {
            throw new ConversionUnsupportedException("Expected Number for LONG");
        }
    }

    private boolean isFloat(Schema schema) {
        return schema.getType() == Schema.Type.FLOAT;
    }
    private Object convertFloat(Object x, Schema schema) {
        if (x instanceof Number) {
            Number n = (Number) x;
            return n.floatValue();
        } else {
            throw new ConversionUnsupportedException("Expected Number for FLOAT");
        }
    }

    private boolean isDouble(Schema schema) {
        return schema.getType() == Schema.Type.DOUBLE;
    }
    private Object convertDouble(Object x, Schema schema) {
        if (x instanceof Number) {
            Number n = (Number) x;
            return n.doubleValue();
        } else {
            throw new ConversionUnsupportedException("Expected Number for DOUBLE");
        }
    }

    private boolean isEnumSymbol(Schema schema) {
        return schema.getType() == Schema.Type.ENUM;
    }
    private Object convertEnumSymbol(Object x, Schema schema) {
        if (x instanceof String) {
            String string = (String)x;
            return new GenericData.EnumSymbol(schema, string);
        } else {
            throw new ConversionUnsupportedException("Expected String for ENUM");
        }
    }

    private boolean isBytes(Schema schema) {
        return schema.getType() == Schema.Type.BYTES;
    }
    private Object convertBytes(Object x, Schema schema) {
        if (x instanceof String) {
            String string = (String)x;
            return ByteBuffer.wrap(string.getBytes(StandardCharsets.ISO_8859_1));
        } else {
            throw new ConversionUnsupportedException("Expected String for BYTES");
        }
    }

    private boolean isString(Schema schema) {
        return schema.getType() == Schema.Type.STRING;
    }
    private Object convertString(Object x, Schema schema) {
        if (x instanceof String) {
            String string = (String) x;
            return string;
        } else {
            throw new ConversionUnsupportedException("Expected String for STRING");
        }
    }

    private boolean isNull(Schema schema) {
        return schema.getType() == Schema.Type.NULL;
    }

    private Object convertNull(Object x, Schema schema) {
        if (x == null) {
            return null;
        } else {
            throw new ConversionUnsupportedException("Expected null for NULL");
        }
    }

}
