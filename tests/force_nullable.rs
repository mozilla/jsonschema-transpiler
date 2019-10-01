use jst::Context;
use jst::{convert_avro, convert_bigquery};
use pretty_assertions::assert_eq;
use serde_json::Value;

fn test_data() -> Value {
    serde_json::from_str(
        r#"
    {
        "type": "object",
        "properties": {
            "array": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "a": {"type": "boolean"}
                    },
                    "required": ["a"]
                }
            },
            "atom": {"type": "integer"},
            "map": {
                "type": "object",
                "additionalProperties": {
                    "type": "object",
                    "properties": {
                        "b": {"type": "boolean"}
                    },
                    "required": ["b"]
                }
            },
            "object": {
                "type": "object",
                "properties": {
                    "c": {"type": "boolean"},
                    "d": {"type": "boolean"}
                },
                "required": ["c", "d"]
            },
            "union": {
                "oneOf": [
                    {
                        "type": "object",
                        "properties": {
                            "e": {"type": "boolean"}
                        },
                        "required": ["e"]
                    },
                    {
                        "type": "object",
                        "properties": {
                            "f": {"type": "boolean"}
                        },
                        "required": ["f"]
                    }
                ]
            },
            "tuple": {
                "type": "array",
                "items": [
                    {"type": "boolean"}
                ],
                "maxItems": 1
            }
        },
        "required": ["atom", "object", "map", "array", "union", "tuple"]
    }
    "#,
    )
    .unwrap()
}

#[test]
fn test_bigquery_force_nullable() {
    let context = Context {
        force_nullable: true,
        tuple_struct: true,
        ..Default::default()
    };

    let expected: Value = serde_json::from_str(
        r#"
        [
            {
                "fields": [
                {
                    "mode": "NULLABLE",
                    "name": "a",
                    "type": "BOOL"
                }
                ],
                "mode": "REPEATED",
                "name": "array",
                "type": "RECORD"
            },
            {
                "mode": "NULLABLE",
                "name": "atom",
                "type": "INT64"
            },
            {
                "fields": [
                {
                    "mode": "NULLABLE",
                    "name": "key",
                    "type": "STRING"
                },
                {
                    "fields": [
                    {
                        "mode": "NULLABLE",
                        "name": "b",
                        "type": "BOOL"
                    }
                    ],
                    "mode": "NULLABLE",
                    "name": "value",
                    "type": "RECORD"
                }
                ],
                "mode": "REPEATED",
                "name": "map",
                "type": "RECORD"
            },
            {
                "fields": [
                {
                    "mode": "NULLABLE",
                    "name": "c",
                    "type": "BOOL"
                },
                {
                    "mode": "NULLABLE",
                    "name": "d",
                    "type": "BOOL"
                }
                ],
                "mode": "NULLABLE",
                "name": "object",
                "type": "RECORD"
            },
            {
                "fields": [
                {
                    "mode": "NULLABLE",
                    "name": "f0_",
                    "type": "BOOL"
                }
                ],
                "mode": "NULLABLE",
                "name": "tuple",
                "type": "RECORD"
            },
            {
                "fields": [
                {
                    "mode": "NULLABLE",
                    "name": "e",
                    "type": "BOOL"
                },
                {
                    "mode": "NULLABLE",
                    "name": "f",
                    "type": "BOOL"
                }
                ],
                "mode": "NULLABLE",
                "name": "union",
                "type": "RECORD"
            }
        ]
        "#,
    )
    .unwrap();

    assert_eq!(expected, convert_bigquery(&test_data(), context));
}

#[test]
fn test_avro_force_nullable() {
    let context = Context {
        force_nullable: true,
        tuple_struct: true,
        ..Default::default()
    };
    let expected: Value = serde_json::from_str(
        r#"
        [
            {"type": "null"},
            {
            "fields": [
                {
                    "default": null,
                    "name": "array",
                    "type": [
                        {"type": "null"},
                        {
                            "items": [
                                {"type": "null"},
                                {
                                    "fields": [
                                        {
                                            "default": null,
                                            "name": "a",
                                            "type": [
                                                {"type": "null"},
                                                {"type": "boolean"}
                                            ]
                                        }
                                    ],
                                    "name": "list",
                                    "namespace": "root.array",
                                    "type": "record"
                                }
                            ],
                            "type": "array"
                        }
                    ]
                },
                {
                    "default": null,
                    "name": "atom",
                    "type": [
                        {"type": "null"},
                        {"type": "long"}
                    ]
                },
                {
                "default": null,
                "name": "map",
                "type": [
                    {"type": "null"},
                    {
                        "type": "map",
                        "values": [
                            {"type": "null"},
                            {
                                "fields": [
                                    {
                                        "default": null,
                                        "name": "b",
                                        "type": [
                                            {"type": "null"},
                                            {"type": "boolean"}
                                        ]
                                    }
                                ],
                                "name": "value",
                                "namespace": "root.map",
                                "type": "record"
                            }
                        ]
                    }
                ]
                },
                {
                "default": null,
                "name": "object",
                "type": [
                    {"type": "null"},
                    {
                        "fields": [
                            {
                            "default": null,
                            "name": "c",
                                "type": [
                                    {"type": "null"},
                                    {"type": "boolean"}
                                ]
                            },
                            {
                            "default": null,
                            "name": "d",
                                "type": [
                                    {"type": "null"},
                                    {"type": "boolean"}
                                ]
                            }
                        ],
                        "name": "object",
                        "namespace": "root",
                        "type": "record"
                    }
                ]
                },
                {
                    "default": null,
                    "name": "tuple",
                    "type": [
                        {"type": "null"},
                        {
                            "name": "tuple",
                            "namespace": "root",
                            "type": "record",
                            "fields": [
                                {
                                    "default": null,
                                    "name": "f0_",
                                    "type": [
                                        {"type": "null"},
                                        {"type": "boolean"}
                                    ]
                                }
                            ]
                        }
                    ]
                },
                {
                "default": null,
                "name": "union",
                "type": [
                    {"type": "null"},
                    {
                        "fields": [
                            {
                            "default": null,
                            "name": "e",
                                "type": [
                                    {"type": "null"},
                                    {"type": "boolean"}
                                ]
                            },
                            {
                            "default": null,
                            "name": "f",
                                "type": [
                                    {"type": "null"},
                                    {"type": "boolean"}
                                ]
                            }
                        ],
                        "name": "union",
                        "namespace": "root",
                        "type": "record"
                    }
                ]
                }
            ],
            "name": "root",
            "type": "record"
            }
        ]
        "#,
    )
    .unwrap();

    assert_eq!(expected, convert_avro(&test_data(), context));
}
