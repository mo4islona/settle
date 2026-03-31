use std::collections::HashMap;

use crate::schema::ast::{AlwaysEmit, BinaryOp, Expr, ReducerBody, WhenBlock};
use crate::types::{Row, RowMap, Value};

use super::ReducerRuntime;

/// Event Rules runtime: evaluates WHEN/THEN blocks with first-match semantics.
pub struct EventRulesRuntime {
    when_blocks: Vec<WhenBlock>,
    always_emit: Option<AlwaysEmit>,
}

impl EventRulesRuntime {
    pub fn new(body: &ReducerBody) -> Self {
        match body {
            ReducerBody::EventRules {
                when_blocks,
                always_emit,
            } => Self {
                when_blocks: when_blocks.clone(),
                always_emit: always_emit.clone(),
            },
            _ => panic!("EventRulesRuntime requires EventRules body"),
        }
    }
}

impl ReducerRuntime for EventRulesRuntime {
    fn process(
        &self,
        state: &mut HashMap<String, Value>,
        row: &Row,
    ) -> crate::error::Result<Vec<RowMap>> {
        let mut output = HashMap::new();
        let mut matched = false;

        // First-match semantics: evaluate WHEN blocks in order
        for block in &self.when_blocks {
            let ctx = EvalContext {
                state,
                row,
                locals: &HashMap::new(),
            };
            if eval_expr(&block.condition, &ctx).is_truthy() {
                // Evaluate LET bindings
                let mut locals = HashMap::new();
                for (name, expr) in &block.lets {
                    let ctx = EvalContext {
                        state,
                        row,
                        locals: &locals,
                    };
                    let val = eval_expr(expr, &ctx);
                    locals.insert(name.clone(), val);
                }

                // Evaluate SET (state mutations)
                for (field, expr) in &block.sets {
                    let ctx = EvalContext {
                        state,
                        row,
                        locals: &locals,
                    };
                    let val = eval_expr(expr, &ctx);
                    state.insert(field.clone(), val);
                }

                // Evaluate EMIT
                for (name, expr) in &block.emits {
                    let ctx = EvalContext {
                        state,
                        row,
                        locals: &locals,
                    };
                    let val = eval_expr(expr, &ctx);
                    output.insert(name.clone(), val);
                }

                matched = true;
                break; // first match wins
            }
        }

        // ALWAYS EMIT — runs regardless of which WHEN matched (or none)
        if let Some(always) = &self.always_emit {
            for (name, expr) in &always.emits {
                let ctx = EvalContext {
                    state,
                    row,
                    locals: &HashMap::new(),
                };
                let val = eval_expr(expr, &ctx);
                output.insert(name.clone(), val);
            }
        }

        if matched || self.always_emit.is_some() {
            Ok(vec![output])
        } else {
            Ok(vec![])
        }
    }
}

struct EvalContext<'a> {
    state: &'a HashMap<String, Value>,
    row: &'a Row,
    locals: &'a HashMap<String, Value>,
}

fn eval_expr(expr: &Expr, ctx: &EvalContext) -> Value {
    match expr {
        Expr::Literal(s) => Value::String(s.clone()),
        Expr::Float(v) => Value::Float64(*v),
        Expr::Int(v) => {
            if *v >= 0 {
                Value::UInt64(*v as u64)
            } else {
                Value::Int64(*v)
            }
        }
        Expr::StateRef(field) => ctx.state.get(field).cloned().unwrap_or(Value::Null),
        Expr::RowRef(field) => ctx.row.get(field).cloned().unwrap_or(Value::Null),
        Expr::ColumnRef(name) => {
            // Check locals first, then row, then state
            if let Some(v) = ctx.locals.get(name) {
                return v.clone();
            }
            if let Some(v) = ctx.row.get(name) {
                return v.clone();
            }
            if let Some(v) = ctx.state.get(name) {
                return v.clone();
            }
            Value::Null
        }
        Expr::BinaryOp { left, op, right } => {
            let lval = eval_expr(left, ctx);
            let rval = eval_expr(right, ctx);
            eval_binary_op(&lval, op, &rval)
        }
        Expr::If {
            condition,
            then_expr,
            else_expr,
        } => {
            if eval_expr(condition, ctx).is_truthy() {
                eval_expr(then_expr, ctx)
            } else {
                eval_expr(else_expr, ctx)
            }
        }
    }
}

fn eval_binary_op(left: &Value, op: &BinaryOp, right: &Value) -> Value {
    match op {
        // Arithmetic
        BinaryOp::Add => arith(left, right, |a, b| a + b),
        BinaryOp::Sub => arith(left, right, |a, b| a - b),
        BinaryOp::Mul => arith(left, right, |a, b| a * b),
        BinaryOp::Div => arith(left, right, |a, b| if b != 0.0 { a / b } else { 0.0 }),

        // Comparison
        BinaryOp::Eq => Value::Boolean(values_eq(left, right)),
        BinaryOp::Neq => Value::Boolean(!values_eq(left, right)),
        BinaryOp::Gt => Value::Boolean(
            values_cmp(left, right).map_or(false, |o| o == std::cmp::Ordering::Greater),
        ),
        BinaryOp::Lt => {
            Value::Boolean(values_cmp(left, right).map_or(false, |o| o == std::cmp::Ordering::Less))
        }
        BinaryOp::Gte => {
            Value::Boolean(values_cmp(left, right).map_or(false, |o| o != std::cmp::Ordering::Less))
        }
        BinaryOp::Lte => Value::Boolean(
            values_cmp(left, right).map_or(false, |o| o != std::cmp::Ordering::Greater),
        ),

        // Logical
        BinaryOp::And => Value::Boolean(left.is_truthy() && right.is_truthy()),
        BinaryOp::Or => Value::Boolean(left.is_truthy() || right.is_truthy()),
    }
}

fn arith(left: &Value, right: &Value, op: fn(f64, f64) -> f64) -> Value {
    match (left.as_f64(), right.as_f64()) {
        (Some(a), Some(b)) => Value::Float64(op(a, b)),
        _ => Value::Null,
    }
}

fn values_eq(left: &Value, right: &Value) -> bool {
    // Support cross-type numeric comparison
    if let (Some(a), Some(b)) = (left.as_f64(), right.as_f64()) {
        return a == b;
    }
    left == right
}

fn values_cmp(left: &Value, right: &Value) -> Option<std::cmp::Ordering> {
    // Numeric comparison across types
    match (left.as_f64(), right.as_f64()) {
        (Some(a), Some(b)) => a.partial_cmp(&b),
        _ => left.partial_cmp(right),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::ast::{AlwaysEmit, BinaryOp as AstBinaryOp, Expr, ReducerBody, WhenBlock};

    fn pnl_reducer_body() -> ReducerBody {
        // PnL tracker from RFC Section 5.6
        ReducerBody::EventRules {
            when_blocks: vec![
                // WHEN row.side = 'buy'
                WhenBlock {
                    condition: Expr::BinaryOp {
                        left: Box::new(Expr::RowRef("side".into())),
                        op: AstBinaryOp::Eq,
                        right: Box::new(Expr::Literal("buy".into())),
                    },
                    lets: vec![],
                    sets: vec![
                        // state.quantity = state.quantity + row.amount
                        (
                            "quantity".into(),
                            Expr::BinaryOp {
                                left: Box::new(Expr::StateRef("quantity".into())),
                                op: AstBinaryOp::Add,
                                right: Box::new(Expr::RowRef("amount".into())),
                            },
                        ),
                        // state.cost_basis = state.cost_basis + row.amount * row.price
                        (
                            "cost_basis".into(),
                            Expr::BinaryOp {
                                left: Box::new(Expr::StateRef("cost_basis".into())),
                                op: AstBinaryOp::Add,
                                right: Box::new(Expr::BinaryOp {
                                    left: Box::new(Expr::RowRef("amount".into())),
                                    op: AstBinaryOp::Mul,
                                    right: Box::new(Expr::RowRef("price".into())),
                                }),
                            },
                        ),
                    ],
                    emits: vec![("trade_pnl".into(), Expr::Int(0))],
                },
                // WHEN row.side = 'sell'
                WhenBlock {
                    condition: Expr::BinaryOp {
                        left: Box::new(Expr::RowRef("side".into())),
                        op: AstBinaryOp::Eq,
                        right: Box::new(Expr::Literal("sell".into())),
                    },
                    lets: vec![
                        // LET avg_cost = state.cost_basis / state.quantity
                        (
                            "avg_cost".into(),
                            Expr::BinaryOp {
                                left: Box::new(Expr::StateRef("cost_basis".into())),
                                op: AstBinaryOp::Div,
                                right: Box::new(Expr::StateRef("quantity".into())),
                            },
                        ),
                    ],
                    sets: vec![
                        // state.quantity = state.quantity - row.amount
                        (
                            "quantity".into(),
                            Expr::BinaryOp {
                                left: Box::new(Expr::StateRef("quantity".into())),
                                op: AstBinaryOp::Sub,
                                right: Box::new(Expr::RowRef("amount".into())),
                            },
                        ),
                        // state.cost_basis = state.cost_basis - row.amount * avg_cost
                        (
                            "cost_basis".into(),
                            Expr::BinaryOp {
                                left: Box::new(Expr::StateRef("cost_basis".into())),
                                op: AstBinaryOp::Sub,
                                right: Box::new(Expr::BinaryOp {
                                    left: Box::new(Expr::RowRef("amount".into())),
                                    op: AstBinaryOp::Mul,
                                    right: Box::new(Expr::ColumnRef("avg_cost".into())),
                                }),
                            },
                        ),
                    ],
                    emits: vec![
                        // EMIT trade_pnl = row.amount * (row.price - avg_cost)
                        (
                            "trade_pnl".into(),
                            Expr::BinaryOp {
                                left: Box::new(Expr::RowRef("amount".into())),
                                op: AstBinaryOp::Mul,
                                right: Box::new(Expr::BinaryOp {
                                    left: Box::new(Expr::RowRef("price".into())),
                                    op: AstBinaryOp::Sub,
                                    right: Box::new(Expr::ColumnRef("avg_cost".into())),
                                }),
                            },
                        ),
                    ],
                },
            ],
            always_emit: Some(AlwaysEmit {
                emits: vec![
                    // state.quantity AS position_size
                    ("position_size".into(), Expr::StateRef("quantity".into())),
                    // IF(state.quantity > 0, state.cost_basis / state.quantity, 0) AS avg_cost
                    (
                        "avg_cost".into(),
                        Expr::If {
                            condition: Box::new(Expr::BinaryOp {
                                left: Box::new(Expr::StateRef("quantity".into())),
                                op: AstBinaryOp::Gt,
                                right: Box::new(Expr::Int(0)),
                            }),
                            then_expr: Box::new(Expr::BinaryOp {
                                left: Box::new(Expr::StateRef("cost_basis".into())),
                                op: AstBinaryOp::Div,
                                right: Box::new(Expr::StateRef("quantity".into())),
                            }),
                            else_expr: Box::new(Expr::Int(0)),
                        },
                    ),
                ],
            }),
        }
    }

    fn make_trade(side: &str, amount: f64, price: f64) -> Row {
        Row::from(HashMap::from([
            ("side".to_string(), Value::String(side.to_string())),
            ("amount".to_string(), Value::Float64(amount)),
            ("price".to_string(), Value::Float64(price)),
        ]))
    }

    #[test]
    fn pnl_tracker_three_trades() {
        // RFC Section 5.6: alice's 3 trades
        let body = pnl_reducer_body();
        let runtime = EventRulesRuntime::new(&body);

        let mut state = HashMap::from([
            ("quantity".to_string(), Value::Float64(0.0)),
            ("cost_basis".to_string(), Value::Float64(0.0)),
        ]);

        // Trade 1: BUY 10 ETH @ $2000
        let row1 = make_trade("buy", 10.0, 2000.0);
        let out1 = runtime
            .process(&mut state, &row1)
            .unwrap()
            .into_iter()
            .next()
            .unwrap();
        assert_eq!(state.get("quantity"), Some(&Value::Float64(10.0)));
        assert_eq!(state.get("cost_basis"), Some(&Value::Float64(20000.0)));
        assert_eq!(out1.get("trade_pnl"), Some(&Value::UInt64(0)));
        assert_eq!(out1.get("position_size"), Some(&Value::Float64(10.0)));
        assert_eq!(out1.get("avg_cost"), Some(&Value::Float64(2000.0)));

        // Trade 2: BUY 5 ETH @ $2100
        let row2 = make_trade("buy", 5.0, 2100.0);
        let out2 = runtime
            .process(&mut state, &row2)
            .unwrap()
            .into_iter()
            .next()
            .unwrap();
        assert_eq!(state.get("quantity"), Some(&Value::Float64(15.0)));
        assert_eq!(state.get("cost_basis"), Some(&Value::Float64(30500.0)));
        assert_eq!(out2.get("trade_pnl"), Some(&Value::UInt64(0)));
        assert_eq!(out2.get("position_size"), Some(&Value::Float64(15.0)));
        let avg_cost = out2.get("avg_cost").unwrap().as_f64().unwrap();
        assert!((avg_cost - 2033.333).abs() < 0.01);

        // Trade 3: SELL 8 ETH @ $2200
        let row3 = make_trade("sell", 8.0, 2200.0);
        let out3 = runtime
            .process(&mut state, &row3)
            .unwrap()
            .into_iter()
            .next()
            .unwrap();

        let trade_pnl = out3.get("trade_pnl").unwrap().as_f64().unwrap();
        // 8 * (2200 - 2033.33) = 1333.33
        assert!((trade_pnl - 1333.33).abs() < 0.01);

        assert_eq!(out3.get("position_size"), Some(&Value::Float64(7.0)));
        let final_avg = out3.get("avg_cost").unwrap().as_f64().unwrap();
        assert!((final_avg - 2033.333).abs() < 0.01);
    }

    #[test]
    fn first_match_wins() {
        let body = ReducerBody::EventRules {
            when_blocks: vec![
                WhenBlock {
                    condition: Expr::BinaryOp {
                        left: Box::new(Expr::RowRef("x".into())),
                        op: AstBinaryOp::Gt,
                        right: Box::new(Expr::Int(0)),
                    },
                    lets: vec![],
                    sets: vec![],
                    emits: vec![("matched".into(), Expr::Literal("first".into()))],
                },
                WhenBlock {
                    condition: Expr::BinaryOp {
                        left: Box::new(Expr::RowRef("x".into())),
                        op: AstBinaryOp::Gt,
                        right: Box::new(Expr::Int(0)),
                    },
                    lets: vec![],
                    sets: vec![],
                    emits: vec![("matched".into(), Expr::Literal("second".into()))],
                },
            ],
            always_emit: None,
        };
        let runtime = EventRulesRuntime::new(&body);
        let mut state = HashMap::new();
        let row = Row::from(HashMap::from([("x".to_string(), Value::Float64(1.0))]));
        let out = runtime
            .process(&mut state, &row)
            .unwrap()
            .into_iter()
            .next()
            .unwrap();
        assert_eq!(out.get("matched"), Some(&Value::String("first".into())));
    }

    #[test]
    fn no_match_no_always_returns_none() {
        let body = ReducerBody::EventRules {
            when_blocks: vec![WhenBlock {
                condition: Expr::BinaryOp {
                    left: Box::new(Expr::RowRef("x".into())),
                    op: AstBinaryOp::Gt,
                    right: Box::new(Expr::Int(100)),
                },
                lets: vec![],
                sets: vec![],
                emits: vec![("out".into(), Expr::Int(1))],
            }],
            always_emit: None,
        };
        let runtime = EventRulesRuntime::new(&body);
        let mut state = HashMap::new();
        let row = Row::from(HashMap::from([("x".to_string(), Value::Float64(1.0))]));
        assert!(runtime.process(&mut state, &row).unwrap().is_empty());
    }

    #[test]
    fn always_emit_without_match() {
        let body = ReducerBody::EventRules {
            when_blocks: vec![WhenBlock {
                condition: Expr::BinaryOp {
                    left: Box::new(Expr::RowRef("x".into())),
                    op: AstBinaryOp::Gt,
                    right: Box::new(Expr::Int(100)),
                },
                lets: vec![],
                sets: vec![],
                emits: vec![],
            }],
            always_emit: Some(AlwaysEmit {
                emits: vec![("always".into(), Expr::Int(42))],
            }),
        };
        let runtime = EventRulesRuntime::new(&body);
        let mut state = HashMap::new();
        let row = Row::from(HashMap::from([("x".to_string(), Value::Float64(1.0))]));
        let out = runtime
            .process(&mut state, &row)
            .unwrap()
            .into_iter()
            .next()
            .unwrap();
        assert_eq!(out.get("always"), Some(&Value::UInt64(42)));
    }

    #[test]
    fn expression_evaluator_arithmetic() {
        let row = Row::from(HashMap::from([("a".to_string(), Value::Float64(10.0))]));
        let ctx = EvalContext {
            state: &HashMap::new(),
            row: &row,
            locals: &HashMap::new(),
        };

        // a + 5
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::RowRef("a".into())),
            op: AstBinaryOp::Add,
            right: Box::new(Expr::Float(5.0)),
        };
        assert_eq!(eval_expr(&expr, &ctx), Value::Float64(15.0));

        // a * 3
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::RowRef("a".into())),
            op: AstBinaryOp::Mul,
            right: Box::new(Expr::Float(3.0)),
        };
        assert_eq!(eval_expr(&expr, &ctx), Value::Float64(30.0));
    }

    #[test]
    fn expression_evaluator_division_by_zero() {
        let row = Row::from(RowMap::new());
        let ctx = EvalContext {
            state: &HashMap::new(),
            row: &row,
            locals: &HashMap::new(),
        };
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Float(10.0)),
            op: AstBinaryOp::Div,
            right: Box::new(Expr::Float(0.0)),
        };
        assert_eq!(eval_expr(&expr, &ctx), Value::Float64(0.0));
    }

    #[test]
    fn expression_evaluator_if() {
        let row = Row::from(RowMap::new());
        let ctx = EvalContext {
            state: &HashMap::from([("qty".to_string(), Value::Float64(5.0))]),
            row: &row,
            locals: &HashMap::new(),
        };
        let expr = Expr::If {
            condition: Box::new(Expr::BinaryOp {
                left: Box::new(Expr::StateRef("qty".into())),
                op: AstBinaryOp::Gt,
                right: Box::new(Expr::Int(0)),
            }),
            then_expr: Box::new(Expr::Literal("positive".into())),
            else_expr: Box::new(Expr::Literal("zero_or_neg".into())),
        };
        assert_eq!(eval_expr(&expr, &ctx), Value::String("positive".into()));
    }
}
