use remap::prelude::*;

#[derive(Clone, Copy, Debug)]
pub struct EncodeJson;

impl Function for EncodeJson {
    fn identifier(&self) -> &'static str {
        "encode_json"
    }

    fn parameters(&self) -> &'static [Parameter] {
        &[Parameter {
            keyword: "value",
            accepts: |_| true,
            required: true,
        }]
    }

    fn compile(&self, mut arguments: ArgumentList) -> Result<Box<dyn Expression>> {
        let value = arguments.required("value")?.boxed();

        Ok(Box::new(EncodeJsonFn { value }))
    }
}

#[derive(Debug, Clone)]
struct EncodeJsonFn {
    value: Box<dyn Expression>,
}

impl Expression for EncodeJsonFn {
    fn execute(&self, state: &mut state::Program, object: &mut dyn Object) -> Result<Value> {
        let value = self.value.execute(state, object)?;

        // With `remap::Value` it's should not be possible to get `Err`.
        match serde_json::to_string(&value) {
            Ok(value) => Ok(value.into()),
            Err(error) => unreachable!("unable encode to json: {}", error),
        }
    }

    fn type_def(&self, state: &state::Compiler) -> TypeDef {
        self.value
            .type_def(state)
            .with_constraint(value::Kind::Bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{DateTime, Utc};
    use regex::Regex;
    use value::Kind;

    test_function![
        encode_json => EncodeJson;

        bytes {
            args: func_args![value: r#"hello"#],
            want: Ok(r#""hello""#),
        }

        integer {
            args: func_args![value: 42],
            want: Ok("42"),
        }

        float {
            args: func_args![value: 42f64],
            want: Ok("42.0"),
        }

        boolean {
            args: func_args![value: false],
            want: Ok("false"),
        }

        map {
            args: func_args![value: map!["field": "value"]],
            want: Ok(r#"{"field":"value"}"#),
        }

        array {
            args: func_args![value: vec![1, 2, 3]],
            want: Ok("[1,2,3]"),
        }

        timestamp {
            args: func_args![
                value: DateTime::parse_from_str("1983 Apr 13 12:09:14.274 +0000", "%Y %b %d %H:%M:%S%.3f %z")
                    .unwrap()
                    .with_timezone(&Utc)
            ],
            want: Ok(r#""1983-04-13 12:09:14.274 UTC""#),

        }

        regex {
            args: func_args![value: Regex::new("^a\\d+$").unwrap()],
            want: Ok(r#""^a\\d+$""#),
        }

        null {
            args: func_args![value: Value::Null],
            want: Ok("null"),
        }
    ];

    test_type_def![
        bytes {
            expr: |_| EncodeJsonFn { value: Literal::from("foo").boxed() },
            def: TypeDef { kind: Kind::Bytes, ..Default::default() },
        }

        integer {
            expr: |_| EncodeJsonFn { value: Literal::from(42).boxed() },
            def: TypeDef { kind: Kind::Bytes, ..Default::default() },
        }

        float {
            expr: |_| EncodeJsonFn { value: Literal::from(42f64).boxed() },
            def: TypeDef { kind: Kind::Bytes, ..Default::default() },
        }

        boolean {
            expr: |_| EncodeJsonFn { value: Literal::from(true).boxed() },
            def: TypeDef { kind: Kind::Bytes, ..Default::default() },
        }

        map {
            expr: |_| EncodeJsonFn { value: map!{}.boxed() },
            def: TypeDef { kind: Kind::Bytes, ..Default::default() },
        }

        array {
            expr: |_| EncodeJsonFn { value: array![].boxed() },
            def: TypeDef { kind: Kind::Bytes, ..Default::default() },
        }

        timestamp {
            expr: |_| EncodeJsonFn { value: Literal::from(chrono::Utc::now()).boxed() },
            def: TypeDef { kind: Kind::Bytes, ..Default::default() },
        }

        regex {
            expr: |_| EncodeJsonFn { value: Literal::from(Regex::new("^a\\d+$").unwrap()).boxed() },
            def: TypeDef { kind: Kind::Bytes, ..Default::default() },
        }

        null {
            expr: |_| EncodeJsonFn { value: Literal::from(()).boxed() },
            def: TypeDef { kind: Kind::Bytes, ..Default::default() },
        }
    ];
}
