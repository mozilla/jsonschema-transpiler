# Examples

This section contains an extended set of examples that you may modify to
understand the differences between different schema formats.

### `atom.json`

```json
{
  "type": "object",
  "properties": {
    "flag": {
      "type": "boolean"
    }
  },
  "required": ["flag"]
}
```

### `atom-nullable.json`

```json
{
  "type": "object",
  "properties": {
    "flag": {
      "type": "boolean"
    }
  }
}
```

### `list.json`

```json
{
  "type": "object",
  "properties": {
    "feature-vector": {
      "type": "array",
      "items": {
        "type": "integer"
      }
    }
  },
  "required": [
    "feature-vector"
  ]
}
```

### `map.json`

```json
{
    "type": "object",
    "properties": {
        "histogram": {
            "type": "object",
            "additionalProperties": {
                "type": "integer"
            }
        }
    },
    "required": [
        "histogram"
    ]
}
```
