# avro-json
Utility to convert between generic Avro and JSON.

The library solves the problem of converting between Avro and JSON when UNION types are used.
In Avro optional value is described as union where at least one type is NULL.

Consider example schema:

```
{
  "type": "record",
  "name": "Test",
  "fields": [
    {
      "name": "id",
      "type": "string"
    },
    {
      "name": "description",
      "type": ["null","string"]
    }
}
```

The natural expected JSON representation looks like the following:

```
{
  "id": "123",
  "description": "Example"
}
```

With native Avro to JSON conversion, however, the same content looks like this:

```
{
  "id": "123",
  "description": {
    "string": "Example"
  }
}
```

This component provides the conversion necessary to transform between Avro and the expected format.
The cost is that the conversion is not always reversible.
