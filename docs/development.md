# Development Notes

This section contains miscellaneous notes around the development of the
transpiler.


### Representation of schemas
Currently, schemas are deserialized directly from their JSON counterparts into
Rust structs and enums using `serde_json`. Enums in Rust are similar to algebraic
data types in functional languages and support robust pattern matching. As such,
a common pattern is to abstract a schema into a type and a tag.

The type forms a set of symbols and the rules for producing a sequence of those
symbols. A simple type could be defined as follows:

```rust
enum Atom {
    Boolean,
    Integer
}

enum Type {
    Null,
    Atom(Atom),
    List(Vec<Type>)
}

// [null, true, [null, -1]]
let root = Type::List(vec![
    Type::Null,
    Type::Atom(Atom::Boolean),
    Type::List(vec![
        Type::Null,
        Type::Atom(Atom::Integer)
    ])
]);
```

While it is possible to generate a schema for a document tree where the ordering
of elements are fixed (by traversing the tree top-down, left-right), schema
validators often assert other properties about the data structure. We may be
interested in asserting the existence of names in a document; to support naming,
we associate each type with a tag.

A tag is attribute data associated with a type. A tag is used as a proxy in the
recursive definition of a type. Traversing a schema can be done by iterating
through all of the tags in order. Tags may also reference other parts of the
tree, which would typically not be possible by directly defining an recursive
enum.


```rust
enum Type {
    Atom,
    List(Vec<Tag>)
}

struct Tag {
    dtype: Type,
    name: String
}

let root = Tag {
    dtype: Type::List(vec![
        Tag { dtype: Type::Atom, name: "foo" },
        Tag { dtype: Type::Atom, name: "bar" },
    ]),
    name: "object"
};
```

By annotating this with the appropriate `serde` attributes, we are able to obtain
the following schema for free:

```json
{
    "name": "object",
    "type": [
        {"name": "foo", "type": "atom"},
        {"name": "bar", "type": "atom"}
    ]
}
```