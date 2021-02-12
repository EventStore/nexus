use crate::util::round_to_precision;
use remap::prelude::*;

#[derive(Clone, Copy, Debug)]
pub struct Floor;

impl Function for Floor {
    fn identifier(&self) -> &'static str {
        "floor"
    }

    fn parameters(&self) -> &'static [Parameter] {
        &[
            Parameter {
                keyword: "value",
                accepts: |v| matches!(v, Value::Float(_) | Value::Integer(_)),
                required: true,
            },
            Parameter {
                keyword: "precision",
                accepts: |v| matches!(v, Value::Integer(_)),
                required: false,
            },
        ]
    }

    fn compile(&self, mut arguments: ArgumentList) -> Result<Box<dyn Expression>> {
        let value = arguments.required("value")?.boxed();
        let precision = arguments.optional("precision").map(Expr::boxed);

        Ok(Box::new(FloorFn { value, precision }))
    }
}

#[derive(Debug, Clone)]
struct FloorFn {
    value: Box<dyn Expression>,
    precision: Option<Box<dyn Expression>>,
}

impl FloorFn {
    #[cfg(test)]
    fn new(value: Box<dyn Expression>, precision: Option<Box<dyn Expression>>) -> Self {
        Self { value, precision }
    }
}

impl Expression for FloorFn {
    fn execute(&self, state: &mut state::Program, object: &mut dyn Object) -> Result<Value> {
        let precision = match &self.precision {
            Some(expr) => expr.execute(state, object)?.try_integer()?,
            None => 0,
        };

        match self.value.execute(state, object)? {
            Value::Float(f) => Ok(round_to_precision(f, precision, f64::floor).into()),
            v @ Value::Integer(_) => Ok(v),
            _ => unreachable!(),
        }
    }

    fn type_def(&self, state: &state::Compiler) -> TypeDef {
        use value::Kind;

        let value_def = self
            .value
            .type_def(state)
            .fallible_unless(Kind::Integer | Kind::Float);
        let precision_def = self
            .precision
            .as_ref()
            .map(|precision| precision.type_def(state).fallible_unless(Kind::Integer));

        value_def
            .clone()
            .merge_optional(precision_def)
            .with_constraint(match value_def.kind {
                v if v.is_float() || v.is_integer() => v,
                _ => Kind::Integer | Kind::Float,
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::map;
    use value::Kind;

    remap::test_type_def![
        value_float {
            expr: |_| FloorFn {
                value: Literal::from(1.0).boxed(),
                precision: None,
            },
            def: TypeDef { kind: Kind::Float, ..Default::default() },
        }

        value_integer {
            expr: |_| FloorFn {
                value: Literal::from(1).boxed(),
                precision: None,
            },
            def: TypeDef { kind: Kind::Integer, ..Default::default() },
        }

        value_float_or_integer {
            expr: |_| FloorFn {
                value: Variable::new("foo".to_owned(), None).boxed(),
                precision: None,
            },
            def: TypeDef { fallible: true, kind: Kind::Integer | Kind::Float, ..Default::default() },
        }

        fallible_precision {
            expr: |_| FloorFn {
                value: Literal::from(1).boxed(),
                precision: Some(Variable::new("foo".to_owned(), None).boxed()),
            },
            def: TypeDef { fallible: true, kind: Kind::Integer, ..Default::default() },
        }
    ];

    #[test]
    fn floor() {
        let cases = vec![
            (
                map!["foo": 1234.2],
                Ok(1234.0.into()),
                FloorFn::new(Box::new(Path::from("foo")), None),
            ),
            (
                map![],
                Ok(1234.0.into()),
                FloorFn::new(Box::new(Literal::from(Value::Float(1234.8))), None),
            ),
            (
                map![],
                Ok(1234.into()),
                FloorFn::new(Box::new(Literal::from(Value::Integer(1234))), None),
            ),
            (
                map![],
                Ok(1234.3.into()),
                FloorFn::new(
                    Box::new(Literal::from(Value::Float(1234.39429))),
                    Some(Box::new(Literal::from(1))),
                ),
            ),
            (
                map![],
                Ok(1234.5678.into()),
                FloorFn::new(
                    Box::new(Literal::from(Value::Float(1234.56789))),
                    Some(Box::new(Literal::from(4))),
                ),
            ),
            (
                map![],
                Ok(9876543210123456789098765432101234567890987654321.98765.into()),
                FloorFn::new(
                    Box::new(Literal::from(
                        9876543210123456789098765432101234567890987654321.987654321,
                    )),
                    Some(Box::new(Literal::from(5))),
                ),
            ),
        ];

        let mut state = state::Program::default();

        for (object, exp, func) in cases {
            let mut object: Value = object.into();
            let got = func
                .execute(&mut state, &mut object)
                .map_err(|e| format!("{:#}", anyhow::anyhow!(e)));

            assert_eq!(got, exp);
        }
    }
}
