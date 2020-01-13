use jst::convert_bigquery;
use jst::{Context, ResolveMethod};
use pretty_assertions::assert_eq;
use serde_json::Value;

#[test]
fn bigquery_test_array_with_atomics() {
    let input_data = r#"
    {
      "items": {
        "type": "integer"
      },
      "type": "array"
    }
    "#;
    let expected_data = r#"
    [
      {
        "mode": "REPEATED",
        "name": "root",
        "type": "INT64"
      }
    ]
    "#;
    let mut context = Context {
        ..Default::default()
    };
    let input: Value = serde_json::from_str(input_data).unwrap();
    let expected: Value = serde_json::from_str(expected_data).unwrap();
    assert_eq!(expected, convert_bigquery(&input, context));

    context.resolve_method = ResolveMethod::Panic;
    convert_bigquery(&input, context);
}

#[test]
fn bigquery_test_array_with_complex() {
    let input_data = r#"
    {
      "items": {
        "properties": {
          "field_1": {
            "type": "string"
          },
          "field_2": {
            "type": "integer"
          }
        },
        "type": "object"
      },
      "type": "array"
    }
    "#;
    let expected_data = r#"
    [
      {
        "fields": [
          {
            "mode": "NULLABLE",
            "name": "field_1",
            "type": "STRING"
          },
          {
            "mode": "NULLABLE",
            "name": "field_2",
            "type": "INT64"
          }
        ],
        "mode": "REPEATED",
        "name": "root",
        "type": "RECORD"
      }
    ]
    "#;
    let mut context = Context {
        ..Default::default()
    };
    let input: Value = serde_json::from_str(input_data).unwrap();
    let expected: Value = serde_json::from_str(expected_data).unwrap();
    assert_eq!(expected, convert_bigquery(&input, context));

    context.resolve_method = ResolveMethod::Panic;
    convert_bigquery(&input, context);
}

#[test]
fn bigquery_test_array_of_array() {
    let input_data = r#"
    {
      "properties": {
        "array": {
          "items": {
            "items": {
              "items": {
                "type": "integer"
              },
              "type": "array"
            },
            "type": "array"
          },
          "type": "array"
        }
      },
      "required": [
        "array"
      ],
      "type": "object"
    }
    "#;
    let expected_data = r#"
    [
      {
        "fields": [
          {
            "fields": [
              {
                "mode": "REPEATED",
                "name": "list",
                "type": "INT64"
              }
            ],
            "mode": "REPEATED",
            "name": "list",
            "type": "RECORD"
          }
        ],
        "mode": "REPEATED",
        "name": "array",
        "type": "RECORD"
      }
    ]
    "#;
    let mut context = Context {
        ..Default::default()
    };
    let input: Value = serde_json::from_str(input_data).unwrap();
    let expected: Value = serde_json::from_str(expected_data).unwrap();
    assert_eq!(expected, convert_bigquery(&input, context));

    context.resolve_method = ResolveMethod::Panic;
    convert_bigquery(&input, context);
}

#[test]
fn bigquery_test_atomic() {
    let input_data = r#"
    {
      "type": "integer"
    }
    "#;
    let expected_data = r#"
    [
      {
        "mode": "REQUIRED",
        "name": "root",
        "type": "INT64"
      }
    ]
    "#;
    let mut context = Context {
        ..Default::default()
    };
    let input: Value = serde_json::from_str(input_data).unwrap();
    let expected: Value = serde_json::from_str(expected_data).unwrap();
    assert_eq!(expected, convert_bigquery(&input, context));

    context.resolve_method = ResolveMethod::Panic;
    convert_bigquery(&input, context);
}

#[test]
fn bigquery_test_atomic_with_null() {
    let input_data = r#"
    {
      "type": [
        "integer",
        "null"
      ]
    }
    "#;
    let expected_data = r#"
    [
      {
        "mode": "NULLABLE",
        "name": "root",
        "type": "INT64"
      }
    ]
    "#;
    let mut context = Context {
        ..Default::default()
    };
    let input: Value = serde_json::from_str(input_data).unwrap();
    let expected: Value = serde_json::from_str(expected_data).unwrap();
    assert_eq!(expected, convert_bigquery(&input, context));

    context.resolve_method = ResolveMethod::Panic;
    convert_bigquery(&input, context);
}

#[test]
#[should_panic]
fn bigquery_test_incompatible_atomic_multitype() {
    let input_data = r#"
    {
      "type": [
        "boolean",
        "integer"
      ]
    }
    "#;
    let expected_data = r#"
    [
      {
        "mode": "REQUIRED",
        "name": "root",
        "type": "STRING"
      }
    ]
    "#;
    let mut context = Context {
        ..Default::default()
    };
    let input: Value = serde_json::from_str(input_data).unwrap();
    let expected: Value = serde_json::from_str(expected_data).unwrap();
    assert_eq!(expected, convert_bigquery(&input, context));

    context.resolve_method = ResolveMethod::Panic;
    convert_bigquery(&input, context);
}

#[test]
#[should_panic]
fn bigquery_test_incompatible_atomic_multitype_with_null() {
    let input_data = r#"
    {
      "type": [
        "boolean",
        "integer",
        "null"
      ]
    }
    "#;
    let expected_data = r#"
    [
      {
        "mode": "NULLABLE",
        "name": "root",
        "type": "STRING"
      }
    ]
    "#;
    let mut context = Context {
        ..Default::default()
    };
    let input: Value = serde_json::from_str(input_data).unwrap();
    let expected: Value = serde_json::from_str(expected_data).unwrap();
    assert_eq!(expected, convert_bigquery(&input, context));

    context.resolve_method = ResolveMethod::Panic;
    convert_bigquery(&input, context);
}

#[test]
fn bigquery_test_datetime() {
    let input_data = r#"
    {
      "format": "date-time",
      "type": "string"
    }
    "#;
    let expected_data = r#"
    [
      {
        "mode": "REQUIRED",
        "name": "root",
        "type": "TIMESTAMP"
      }
    ]
    "#;
    let mut context = Context {
        ..Default::default()
    };
    let input: Value = serde_json::from_str(input_data).unwrap();
    let expected: Value = serde_json::from_str(expected_data).unwrap();
    assert_eq!(expected, convert_bigquery(&input, context));

    context.resolve_method = ResolveMethod::Panic;
    convert_bigquery(&input, context);
}

#[test]
fn bigquery_test_bytes_format() {
    let input_data = r#"
    {
      "format": "bytes",
      "type": "string"
    }
    "#;
    let expected_data = r#"
    [
      {
        "mode": "REQUIRED",
        "name": "root",
        "type": "BYTES"
      }
    ]
    "#;
    let mut context = Context {
        ..Default::default()
    };
    let input: Value = serde_json::from_str(input_data).unwrap();
    let expected: Value = serde_json::from_str(expected_data).unwrap();
    assert_eq!(expected, convert_bigquery(&input, context));

    context.resolve_method = ResolveMethod::Panic;
    convert_bigquery(&input, context);
}

#[test]
fn bigquery_test_atomic_with_description() {
    let input_data = r#"
    {
      "description": "test description",
      "type": "integer"
    }
    "#;
    let expected_data = r#"
    [
      {
        "description": "test description",
        "mode": "REQUIRED",
        "name": "root",
        "type": "INT64"
      }
    ]
    "#;
    let mut context = Context {
        ..Default::default()
    };
    let input: Value = serde_json::from_str(input_data).unwrap();
    let expected: Value = serde_json::from_str(expected_data).unwrap();
    assert_eq!(expected, convert_bigquery(&input, context));

    context.resolve_method = ResolveMethod::Panic;
    convert_bigquery(&input, context);
}

#[test]
fn bigquery_test_atomic_with_description_and_title() {
    let input_data = r#"
    {
      "description": "test description",
      "title": "test title",
      "type": "integer"
    }
    "#;
    let expected_data = r#"
    [
      {
        "description": "test title - test description",
        "mode": "REQUIRED",
        "name": "root",
        "type": "INT64"
      }
    ]
    "#;
    let mut context = Context {
        ..Default::default()
    };
    let input: Value = serde_json::from_str(input_data).unwrap();
    let expected: Value = serde_json::from_str(expected_data).unwrap();
    assert_eq!(expected, convert_bigquery(&input, context));

    context.resolve_method = ResolveMethod::Panic;
    convert_bigquery(&input, context);
}

#[test]
fn bigquery_test_atomic_with_title() {
    let input_data = r#"
    {
      "title": "test title",
      "type": "integer"
    }
    "#;
    let expected_data = r#"
    [
      {
        "description": "test title",
        "mode": "REQUIRED",
        "name": "root",
        "type": "INT64"
      }
    ]
    "#;
    let mut context = Context {
        ..Default::default()
    };
    let input: Value = serde_json::from_str(input_data).unwrap();
    let expected: Value = serde_json::from_str(expected_data).unwrap();
    assert_eq!(expected, convert_bigquery(&input, context));

    context.resolve_method = ResolveMethod::Panic;
    convert_bigquery(&input, context);
}

#[test]
fn bigquery_test_map_with_atomics() {
    let input_data = r#"
    {
      "additionalProperties": {
        "type": "integer"
      },
      "type": "object"
    }
    "#;
    let expected_data = r#"
    [
      {
        "fields": [
          {
            "mode": "REQUIRED",
            "name": "key",
            "type": "STRING"
          },
          {
            "mode": "REQUIRED",
            "name": "value",
            "type": "INT64"
          }
        ],
        "mode": "REPEATED",
        "name": "root",
        "type": "RECORD"
      }
    ]
    "#;
    let mut context = Context {
        ..Default::default()
    };
    let input: Value = serde_json::from_str(input_data).unwrap();
    let expected: Value = serde_json::from_str(expected_data).unwrap();
    assert_eq!(expected, convert_bigquery(&input, context));

    context.resolve_method = ResolveMethod::Panic;
    convert_bigquery(&input, context);
}

#[test]
fn bigquery_test_map_with_complex() {
    let input_data = r#"
    {
      "additionalProperties": {
        "description": "object description",
        "properties": {
          "field_1": {
            "description": "field description",
            "type": "string"
          },
          "field_2": {
            "type": "integer"
          }
        },
        "type": "object"
      },
      "description": "root description",
      "type": "object"
    }
    "#;
    let expected_data = r#"
    [
      {
        "description": "root description",
        "fields": [
          {
            "mode": "REQUIRED",
            "name": "key",
            "type": "STRING"
          },
          {
            "description": "object description",
            "fields": [
              {
                "description": "field description",
                "mode": "NULLABLE",
                "name": "field_1",
                "type": "STRING"
              },
              {
                "mode": "NULLABLE",
                "name": "field_2",
                "type": "INT64"
              }
            ],
            "mode": "REQUIRED",
            "name": "value",
            "type": "RECORD"
          }
        ],
        "mode": "REPEATED",
        "name": "root",
        "type": "RECORD"
      }
    ]
    "#;
    let mut context = Context {
        ..Default::default()
    };
    let input: Value = serde_json::from_str(input_data).unwrap();
    let expected: Value = serde_json::from_str(expected_data).unwrap();
    assert_eq!(expected, convert_bigquery(&input, context));

    context.resolve_method = ResolveMethod::Panic;
    convert_bigquery(&input, context);
}

#[test]
fn bigquery_test_map_with_pattern_properties() {
    let input_data = r#"
    {
      "additionalProperties": false,
      "patternProperties": {
        ".+": {
          "type": "integer"
        }
      },
      "type": "object"
    }
    "#;
    let expected_data = r#"
    [
      {
        "fields": [
          {
            "mode": "REQUIRED",
            "name": "key",
            "type": "STRING"
          },
          {
            "mode": "REQUIRED",
            "name": "value",
            "type": "INT64"
          }
        ],
        "mode": "REPEATED",
        "name": "root",
        "type": "RECORD"
      }
    ]
    "#;
    let mut context = Context {
        ..Default::default()
    };
    let input: Value = serde_json::from_str(input_data).unwrap();
    let expected: Value = serde_json::from_str(expected_data).unwrap();
    assert_eq!(expected, convert_bigquery(&input, context));

    context.resolve_method = ResolveMethod::Panic;
    convert_bigquery(&input, context);
}

#[test]
fn bigquery_test_map_with_pattern_and_additional_properties() {
    let input_data = r#"
    {
      "additionalProperties": {
        "type": "integer"
      },
      "patternProperties": {
        ".+": {
          "type": "integer"
        }
      },
      "type": "object"
    }
    "#;
    let expected_data = r#"
    [
      {
        "fields": [
          {
            "mode": "REQUIRED",
            "name": "key",
            "type": "STRING"
          },
          {
            "mode": "REQUIRED",
            "name": "value",
            "type": "INT64"
          }
        ],
        "mode": "REPEATED",
        "name": "root",
        "type": "RECORD"
      }
    ]
    "#;
    let mut context = Context {
        ..Default::default()
    };
    let input: Value = serde_json::from_str(input_data).unwrap();
    let expected: Value = serde_json::from_str(expected_data).unwrap();
    assert_eq!(expected, convert_bigquery(&input, context));

    context.resolve_method = ResolveMethod::Panic;
    convert_bigquery(&input, context);
}

#[test]
#[should_panic]
fn bigquery_test_incompatible_map_with_pattern_properties() {
    let input_data = r#"
    {
      "additionalProperties": false,
      "patternProperties": {
        "^I_": {
          "type": "integer"
        },
        "^S_": {
          "type": "string"
        }
      },
      "type": "object"
    }
    "#;
    let expected_data = r#"
    [
      {
        "fields": [
          {
            "mode": "REQUIRED",
            "name": "key",
            "type": "STRING"
          },
          {
            "mode": "REQUIRED",
            "name": "value",
            "type": "STRING"
          }
        ],
        "mode": "REPEATED",
        "name": "root",
        "type": "RECORD"
      }
    ]
    "#;
    let mut context = Context {
        ..Default::default()
    };
    let input: Value = serde_json::from_str(input_data).unwrap();
    let expected: Value = serde_json::from_str(expected_data).unwrap();
    assert_eq!(expected, convert_bigquery(&input, context));

    context.resolve_method = ResolveMethod::Panic;
    convert_bigquery(&input, context);
}

#[test]
#[should_panic]
fn bigquery_test_incompatible_map_with_pattern_and_additional_properties() {
    let input_data = r#"
    {
      "additionalProperties": {
        "type": "integer"
      },
      "patternProperties": {
        ".+": {
          "type": "string"
        }
      },
      "type": "object"
    }
    "#;
    let expected_data = r#"
    [
      {
        "fields": [
          {
            "mode": "REQUIRED",
            "name": "key",
            "type": "STRING"
          },
          {
            "mode": "REQUIRED",
            "name": "value",
            "type": "STRING"
          }
        ],
        "mode": "REPEATED",
        "name": "root",
        "type": "RECORD"
      }
    ]
    "#;
    let mut context = Context {
        ..Default::default()
    };
    let input: Value = serde_json::from_str(input_data).unwrap();
    let expected: Value = serde_json::from_str(expected_data).unwrap();
    assert_eq!(expected, convert_bigquery(&input, context));

    context.resolve_method = ResolveMethod::Panic;
    convert_bigquery(&input, context);
}

#[test]
fn bigquery_test_object_with_atomics_is_sorted() {
    let input_data = r#"
    {
      "properties": {
        "field_1": {
          "type": "integer"
        },
        "field_2": {
          "type": "string"
        },
        "field_3": {
          "type": "boolean"
        },
        "field_4": {
          "type": "number"
        }
      },
      "type": "object"
    }
    "#;
    let expected_data = r#"
    [
      {
        "mode": "NULLABLE",
        "name": "field_1",
        "type": "INT64"
      },
      {
        "mode": "NULLABLE",
        "name": "field_2",
        "type": "STRING"
      },
      {
        "mode": "NULLABLE",
        "name": "field_3",
        "type": "BOOL"
      },
      {
        "mode": "NULLABLE",
        "name": "field_4",
        "type": "FLOAT64"
      }
    ]
    "#;
    let mut context = Context {
        ..Default::default()
    };
    let input: Value = serde_json::from_str(input_data).unwrap();
    let expected: Value = serde_json::from_str(expected_data).unwrap();
    assert_eq!(expected, convert_bigquery(&input, context));

    context.resolve_method = ResolveMethod::Panic;
    convert_bigquery(&input, context);
}

#[test]
fn bigquery_test_object_with_atomics_required() {
    let input_data = r#"
    {
      "properties": {
        "field_1": {
          "type": "integer"
        },
        "field_2": {
          "type": "string"
        },
        "field_3": {
          "type": "boolean"
        }
      },
      "required": [
        "field_1",
        "field_3"
      ],
      "type": "object"
    }
    "#;
    let expected_data = r#"
    [
      {
        "mode": "REQUIRED",
        "name": "field_1",
        "type": "INT64"
      },
      {
        "mode": "NULLABLE",
        "name": "field_2",
        "type": "STRING"
      },
      {
        "mode": "REQUIRED",
        "name": "field_3",
        "type": "BOOL"
      }
    ]
    "#;
    let mut context = Context {
        ..Default::default()
    };
    let input: Value = serde_json::from_str(input_data).unwrap();
    let expected: Value = serde_json::from_str(expected_data).unwrap();
    assert_eq!(expected, convert_bigquery(&input, context));

    context.resolve_method = ResolveMethod::Panic;
    convert_bigquery(&input, context);
}

#[test]
fn bigquery_test_object_with_atomics_required_with_null() {
    let input_data = r#"
    {
      "properties": {
        "field_1": {
          "type": [
            "integer",
            "null"
          ]
        },
        "field_2": {
          "type": "string"
        },
        "field_3": {
          "type": "boolean"
        }
      },
      "required": [
        "field_1",
        "field_3"
      ],
      "type": "object"
    }
    "#;
    let expected_data = r#"
    [
      {
        "mode": "NULLABLE",
        "name": "field_1",
        "type": "INT64"
      },
      {
        "mode": "NULLABLE",
        "name": "field_2",
        "type": "STRING"
      },
      {
        "mode": "REQUIRED",
        "name": "field_3",
        "type": "BOOL"
      }
    ]
    "#;
    let mut context = Context {
        ..Default::default()
    };
    let input: Value = serde_json::from_str(input_data).unwrap();
    let expected: Value = serde_json::from_str(expected_data).unwrap();
    assert_eq!(expected, convert_bigquery(&input, context));

    context.resolve_method = ResolveMethod::Panic;
    convert_bigquery(&input, context);
}

#[test]
fn bigquery_test_object_with_complex() {
    let input_data = r#"
    {
      "properties": {
        "namespace_1": {
          "properties": {
            "field_1": {
              "description": "field description",
              "type": "string"
            },
            "field_2": {
              "type": "integer"
            }
          },
          "type": "object"
        }
      },
      "type": "object"
    }
    "#;
    let expected_data = r#"
    [
      {
        "fields": [
          {
            "description": "field description",
            "mode": "NULLABLE",
            "name": "field_1",
            "type": "STRING"
          },
          {
            "mode": "NULLABLE",
            "name": "field_2",
            "type": "INT64"
          }
        ],
        "mode": "NULLABLE",
        "name": "namespace_1",
        "type": "RECORD"
      }
    ]
    "#;
    let mut context = Context {
        ..Default::default()
    };
    let input: Value = serde_json::from_str(input_data).unwrap();
    let expected: Value = serde_json::from_str(expected_data).unwrap();
    assert_eq!(expected, convert_bigquery(&input, context));

    context.resolve_method = ResolveMethod::Panic;
    convert_bigquery(&input, context);
}

#[test]
#[should_panic]
fn bigquery_test_object_empty_record() {
    let input_data = r#"
    {
      "properties": {},
      "type": "object"
    }
    "#;
    let expected_data = r#"
    [
      {
        "mode": "REQUIRED",
        "name": "root",
        "type": "STRING"
      }
    ]
    "#;
    let mut context = Context {
        ..Default::default()
    };
    let input: Value = serde_json::from_str(input_data).unwrap();
    let expected: Value = serde_json::from_str(expected_data).unwrap();
    assert_eq!(expected, convert_bigquery(&input, context));

    context.resolve_method = ResolveMethod::Panic;
    convert_bigquery(&input, context);
}

#[test]
fn bigquery_test_oneof_atomic() {
    let input_data = r#"
    {
      "oneOf": [
        {
          "type": "integer"
        },
        {
          "type": "integer"
        }
      ]
    }
    "#;
    let expected_data = r#"
    [
      {
        "mode": "REQUIRED",
        "name": "root",
        "type": "INT64"
      }
    ]
    "#;
    let mut context = Context {
        ..Default::default()
    };
    let input: Value = serde_json::from_str(input_data).unwrap();
    let expected: Value = serde_json::from_str(expected_data).unwrap();
    assert_eq!(expected, convert_bigquery(&input, context));

    context.resolve_method = ResolveMethod::Panic;
    convert_bigquery(&input, context);
}

#[test]
fn bigquery_test_oneof_atomic_with_null() {
    let input_data = r#"
    {
      "oneOf": [
        {
          "type": "integer"
        },
        {
          "type": "null"
        }
      ]
    }
    "#;
    let expected_data = r#"
    [
      {
        "mode": "NULLABLE",
        "name": "root",
        "type": "INT64"
      }
    ]
    "#;
    let mut context = Context {
        ..Default::default()
    };
    let input: Value = serde_json::from_str(input_data).unwrap();
    let expected: Value = serde_json::from_str(expected_data).unwrap();
    assert_eq!(expected, convert_bigquery(&input, context));

    context.resolve_method = ResolveMethod::Panic;
    convert_bigquery(&input, context);
}

#[test]
#[should_panic]
fn bigquery_test_incompatible_oneof_atomic() {
    let input_data = r#"
    {
      "oneOf": [
        {
          "type": "integer"
        },
        {
          "type": "boolean"
        }
      ]
    }
    "#;
    let expected_data = r#"
    [
      {
        "mode": "REQUIRED",
        "name": "root",
        "type": "STRING"
      }
    ]
    "#;
    let mut context = Context {
        ..Default::default()
    };
    let input: Value = serde_json::from_str(input_data).unwrap();
    let expected: Value = serde_json::from_str(expected_data).unwrap();
    assert_eq!(expected, convert_bigquery(&input, context));

    context.resolve_method = ResolveMethod::Panic;
    convert_bigquery(&input, context);
}

#[test]
#[should_panic]
fn bigquery_test_incompatible_oneof_atomic_with_null() {
    let input_data = r#"
    {
      "oneOf": [
        {
          "type": [
            "integer",
            "null"
          ]
        },
        {
          "type": "boolean"
        }
      ]
    }
    "#;
    let expected_data = r#"
    [
      {
        "mode": "NULLABLE",
        "name": "root",
        "type": "STRING"
      }
    ]
    "#;
    let mut context = Context {
        ..Default::default()
    };
    let input: Value = serde_json::from_str(input_data).unwrap();
    let expected: Value = serde_json::from_str(expected_data).unwrap();
    assert_eq!(expected, convert_bigquery(&input, context));

    context.resolve_method = ResolveMethod::Panic;
    convert_bigquery(&input, context);
}

#[test]
fn bigquery_test_oneof_object_with_atomics() {
    let input_data = r#"
    {
      "oneOf": [
        {
          "properties": {
            "field_1": {
              "type": "integer"
            },
            "field_2": {
              "type": "integer"
            }
          },
          "type": "object"
        },
        {
          "properties": {
            "field_1": {
              "type": "integer"
            },
            "field_2": {
              "type": "integer"
            }
          },
          "type": "object"
        }
      ]
    }
    "#;
    let expected_data = r#"
    [
      {
        "mode": "NULLABLE",
        "name": "field_1",
        "type": "INT64"
      },
      {
        "mode": "NULLABLE",
        "name": "field_2",
        "type": "INT64"
      }
    ]
    "#;
    let mut context = Context {
        ..Default::default()
    };
    let input: Value = serde_json::from_str(input_data).unwrap();
    let expected: Value = serde_json::from_str(expected_data).unwrap();
    assert_eq!(expected, convert_bigquery(&input, context));

    context.resolve_method = ResolveMethod::Panic;
    convert_bigquery(&input, context);
}

#[test]
fn bigquery_test_oneof_object_merge() {
    let input_data = r#"
    {
      "oneOf": [
        {
          "properties": {
            "field_1": {
              "type": "integer"
            },
            "field_3": {
              "type": "number"
            }
          },
          "type": "object"
        },
        {
          "properties": {
            "field_2": {
              "type": "boolean"
            },
            "field_3": {
              "type": "number"
            }
          },
          "type": "object"
        }
      ]
    }
    "#;
    let expected_data = r#"
    [
      {
        "mode": "NULLABLE",
        "name": "field_1",
        "type": "INT64"
      },
      {
        "mode": "NULLABLE",
        "name": "field_2",
        "type": "BOOL"
      },
      {
        "mode": "NULLABLE",
        "name": "field_3",
        "type": "FLOAT64"
      }
    ]
    "#;
    let mut context = Context {
        ..Default::default()
    };
    let input: Value = serde_json::from_str(input_data).unwrap();
    let expected: Value = serde_json::from_str(expected_data).unwrap();
    assert_eq!(expected, convert_bigquery(&input, context));

    context.resolve_method = ResolveMethod::Panic;
    convert_bigquery(&input, context);
}

#[test]
fn bigquery_test_oneof_object_merge_with_complex() {
    let input_data = r#"
    {
      "oneOf": [
        {
          "properties": {
            "namespace_1": {
              "properties": {
                "field_1": {
                  "type": "integer"
                },
                "field_3": {
                  "type": "number"
                }
              },
              "type": "object"
            }
          },
          "type": "object"
        },
        {
          "properties": {
            "namespace_1": {
              "properties": {
                "field_2": {
                  "type": "boolean"
                },
                "field_3": {
                  "type": "number"
                }
              },
              "type": "object"
            }
          },
          "type": "object"
        },
        {
          "properties": {
            "field_4": {
              "type": "boolean"
            },
            "field_5": {
              "type": "number"
            }
          },
          "type": "object"
        }
      ]
    }
    "#;
    let expected_data = r#"
    [
      {
        "mode": "NULLABLE",
        "name": "field_4",
        "type": "BOOL"
      },
      {
        "mode": "NULLABLE",
        "name": "field_5",
        "type": "FLOAT64"
      },
      {
        "fields": [
          {
            "mode": "NULLABLE",
            "name": "field_1",
            "type": "INT64"
          },
          {
            "mode": "NULLABLE",
            "name": "field_2",
            "type": "BOOL"
          },
          {
            "mode": "NULLABLE",
            "name": "field_3",
            "type": "FLOAT64"
          }
        ],
        "mode": "NULLABLE",
        "name": "namespace_1",
        "type": "RECORD"
      }
    ]
    "#;
    let mut context = Context {
        ..Default::default()
    };
    let input: Value = serde_json::from_str(input_data).unwrap();
    let expected: Value = serde_json::from_str(expected_data).unwrap();
    assert_eq!(expected, convert_bigquery(&input, context));

    context.resolve_method = ResolveMethod::Panic;
    convert_bigquery(&input, context);
}

#[test]
#[should_panic]
fn bigquery_test_incompatible_oneof_atomic_and_object() {
    let input_data = r#"
    {
      "oneOf": [
        {
          "type": "integer"
        },
        {
          "properties": {
            "field_1": {
              "type": "integer"
            }
          },
          "type": "object"
        }
      ]
    }
    "#;
    let expected_data = r#"
    [
      {
        "mode": "REQUIRED",
        "name": "root",
        "type": "STRING"
      }
    ]
    "#;
    let mut context = Context {
        ..Default::default()
    };
    let input: Value = serde_json::from_str(input_data).unwrap();
    let expected: Value = serde_json::from_str(expected_data).unwrap();
    assert_eq!(expected, convert_bigquery(&input, context));

    context.resolve_method = ResolveMethod::Panic;
    convert_bigquery(&input, context);
}

#[test]
#[should_panic]
fn bigquery_test_incompatible_oneof_object() {
    let input_data = r#"
    {
      "oneOf": [
        {
          "properties": {
            "field_1": {
              "type": "integer"
            }
          },
          "type": "object"
        },
        {
          "properties": {
            "field_1": {
              "type": "boolean"
            }
          },
          "type": "object"
        }
      ]
    }
    "#;
    let expected_data = r#"
    [
      {
        "mode": "REQUIRED",
        "name": "root",
        "type": "STRING"
      }
    ]
    "#;
    let mut context = Context {
        ..Default::default()
    };
    let input: Value = serde_json::from_str(input_data).unwrap();
    let expected: Value = serde_json::from_str(expected_data).unwrap();
    assert_eq!(expected, convert_bigquery(&input, context));

    context.resolve_method = ResolveMethod::Panic;
    convert_bigquery(&input, context);
}

#[test]
#[should_panic]
fn bigquery_test_incompatible_oneof_object_with_complex() {
    let input_data = r#"
    {
      "oneOf": [
        {
          "properties": {
            "namespace_1": {
              "properties": {
                "field_1": {
                  "type": "string"
                },
                "field_2": {
                  "type": "integer"
                }
              },
              "type": "object"
            }
          },
          "type": "object"
        },
        {
          "properties": {
            "namespace_1": {
              "properties": {
                "field_1": {
                  "type": "boolean"
                },
                "field_2": {
                  "type": "integer"
                }
              },
              "type": "object"
            }
          },
          "type": "object"
        }
      ]
    }
    "#;
    let expected_data = r#"
    [
      {
        "mode": "REQUIRED",
        "name": "root",
        "type": "STRING"
      }
    ]
    "#;
    let mut context = Context {
        ..Default::default()
    };
    let input: Value = serde_json::from_str(input_data).unwrap();
    let expected: Value = serde_json::from_str(expected_data).unwrap();
    assert_eq!(expected, convert_bigquery(&input, context));

    context.resolve_method = ResolveMethod::Panic;
    convert_bigquery(&input, context);
}

#[test]
fn bigquery_test_oneof_object_merge_nullability() {
    let input_data = r#"
    {
      "oneOf": [
        {
          "properties": {
            "shared": {
              "type": "integer"
            },
            "type_a": {
              "type": "integer"
            }
          },
          "required": [
            "shared",
            "type_a"
          ]
        },
        {
          "properties": {
            "shared": {
              "type": "integer"
            },
            "type_b": {
              "type": "integer"
            }
          },
          "required": [
            "shared",
            "type_b"
          ]
        }
      ]
    }
    "#;
    let expected_data = r#"
    [
      {
        "mode": "REQUIRED",
        "name": "shared",
        "type": "INT64"
      },
      {
        "mode": "NULLABLE",
        "name": "type_a",
        "type": "INT64"
      },
      {
        "mode": "NULLABLE",
        "name": "type_b",
        "type": "INT64"
      }
    ]
    "#;
    let mut context = Context {
        ..Default::default()
    };
    let input: Value = serde_json::from_str(input_data).unwrap();
    let expected: Value = serde_json::from_str(expected_data).unwrap();
    assert_eq!(expected, convert_bigquery(&input, context));

    context.resolve_method = ResolveMethod::Panic;
    convert_bigquery(&input, context);
}
