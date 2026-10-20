use std::collections::HashMap;

use crate::error::Error;
use crate::types::ColumnType;

use super::ast::*;

use sqlparser::ast as sql;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

pub fn parse_schema(input: &str) -> Result<Schema, Error> {
    // Split out CREATE MODULE blocks before sqlparser sees them
    let (input_part, modules) = extract_modules(input)?;

    // Split out CREATE REDUCER blocks before sqlparser sees them
    let (sql_part, reducers) = extract_reducers(&input_part)?;

    // Strip WINDOW SLIDING clauses before sqlparser sees them
    let (sql_part, sliding_windows) = extract_sliding_windows(&sql_part)?;

    // Preprocess: replace "CREATE VIRTUAL TABLE" with "CREATE TABLE"
    // and track which table names are virtual.
    let (sql_part, virtual_tables) = extract_virtual_tables(&sql_part);

    let dialect = GenericDialect {};
    let statements = Parser::parse_sql(&dialect, &sql_part)
        .map_err(|e| Error::Schema(format!("SQL parse error: {e}")))?;

    let mut tables = Vec::new();
    let mut materialized_views = Vec::new();

    for stmt in statements {
        match stmt {
            sql::Statement::CreateTable(ct) => {
                let mut def = parse_create_table(&ct)?;
                if virtual_tables.contains(&def.name) {
                    def.virtual_table = true;
                }
                tables.push(def);
            }
            sql::Statement::CreateView {
                or_replace,
                materialized: true,
                name,
                columns,
                query,
                options,
                cluster_by,
                comment,
                with_no_schema_binding,
                if_not_exists,
                temporary,
                to,
                params,
            } => {
                let refused = [
                    (!columns.is_empty(), "a column list"),
                    (to.is_some(), "a TO table"),
                    (or_replace, "OR REPLACE"),
                    (temporary, "TEMPORARY"),
                    (if_not_exists, "IF NOT EXISTS"),
                    (options != sql::CreateTableOptions::None, "OPTIONS"),
                    (!cluster_by.is_empty(), "CLUSTER BY"),
                    (comment.is_some(), "COMMENT"),
                    (with_no_schema_binding, "WITH NO SCHEMA BINDING"),
                    (params.is_some(), "ALGORITHM, DEFINER or SQL SECURITY"),
                ];
                if let Some((_, option)) = refused.iter().find(|(present, _)| *present) {
                    return Err(Error::Schema(format!(
                        "MV '{name}': {option} is not supported in CREATE MATERIALIZED VIEW"
                    )));
                }
                let mut mv = parse_create_mv(&name, &query)?;
                if let Some(sw) = sliding_windows.get(&mv.name) {
                    mv.sliding_window = Some(sw.clone());
                }
                materialized_views.push(mv);
            }
            _ => {
                return Err(Error::Schema(format!("unsupported statement: {stmt}")));
            }
        }
    }

    let schema = Schema {
        tables,
        modules,
        reducers,
        materialized_views,
    };
    validate_schema(&schema)?;
    Ok(schema)
}

fn parse_create_table(ct: &sql::CreateTable) -> Result<TableDef, Error> {
    let name = ct.name.to_string();
    let mut columns = Vec::new();
    for col in &ct.columns {
        let column_type = map_column_type(&col.data_type, &col.name)?;
        columns.push(ColumnDef {
            name: col.name.value.clone(),
            column_type,
        });
    }
    Ok(TableDef {
        name,
        columns,
        virtual_table: false,
    })
}

/// Replace `CREATE VIRTUAL TABLE` with `CREATE TABLE` and return the set
/// of table names that were marked virtual.
fn extract_virtual_tables(input: &str) -> (String, Vec<String>) {
    let mut result = String::with_capacity(input.len());
    let mut virtual_names = Vec::new();
    let upper = input.to_ascii_uppercase();
    let mut pos = 0;

    while pos < input.len() {
        if let Some(offset) = upper[pos..].find("CREATE VIRTUAL TABLE") {
            let abs = pos + offset;
            // Ensure word boundary (start of input or whitespace/semicolon before)
            if abs > 0
                && !input.as_bytes()[abs - 1].is_ascii_whitespace()
                && input.as_bytes()[abs - 1] != b';'
            {
                result.push_str(&input[pos..=abs]);
                pos = abs + 1;
                continue;
            }
            result.push_str(&input[pos..abs]);
            // Skip "CREATE VIRTUAL TABLE" → emit "CREATE TABLE"
            let after_keyword = abs + "CREATE VIRTUAL TABLE".len();
            result.push_str("CREATE TABLE");
            // Extract table name
            let rest = &input[after_keyword..];
            let name_start = rest.find(|c: char| !c.is_ascii_whitespace()).unwrap_or(0);
            let name_end = rest[name_start..]
                .find(|c: char| !c.is_ascii_alphanumeric() && c != '_')
                .map(|i| name_start + i)
                .unwrap_or(rest.len());
            let name = rest[name_start..name_end].to_string();
            if !name.is_empty() {
                virtual_names.push(name);
            }
            pos = after_keyword;
        } else {
            result.push_str(&input[pos..]);
            break;
        }
    }

    (result, virtual_names)
}

/// Strip `WINDOW SLIDING INTERVAL <N> <UNIT> BY <column>` clauses from MV
/// definitions before sqlparser sees them. Returns cleaned SQL and a map of
/// MV name → SlidingWindowDef.
fn extract_sliding_windows(
    input: &str,
) -> Result<(String, HashMap<String, SlidingWindowDef>), Error> {
    let mut result = String::with_capacity(input.len());
    let mut windows = HashMap::new();
    let upper = input.to_ascii_uppercase();
    let mut pos = 0;

    while pos < input.len() {
        if let Some(offset) = upper[pos..].find("WINDOW SLIDING") {
            let abs = pos + offset;
            // Ensure word boundary
            if abs > 0
                && !input.as_bytes()[abs - 1].is_ascii_whitespace()
                && input.as_bytes()[abs - 1] != b';'
            {
                result.push_str(&input[pos..=abs]);
                pos = abs + 1;
                continue;
            }

            // Copy everything before the WINDOW SLIDING clause
            result.push_str(&input[pos..abs]);

            // Parse: WINDOW SLIDING INTERVAL <N> <UNIT> BY <column>
            let rest = &upper[abs + "WINDOW SLIDING".len()..];

            // Skip whitespace and expect INTERVAL
            let trimmed = rest.trim_start();
            if !trimmed.starts_with("INTERVAL") {
                return Err(Error::Schema(
                    "WINDOW SLIDING: expected INTERVAL after SLIDING".into(),
                ));
            }
            let after_interval = &trimmed["INTERVAL".len()..];
            // Parse number
            let after_interval_trimmed = after_interval.trim_start();
            let num_end = after_interval_trimmed
                .find(|c: char| !c.is_ascii_digit())
                .unwrap_or(after_interval_trimmed.len());
            if num_end == 0 {
                return Err(Error::Schema(
                    "WINDOW SLIDING: expected numeric interval value".into(),
                ));
            }
            let n: u64 = after_interval_trimmed[..num_end]
                .parse()
                .map_err(|_| Error::Schema("WINDOW SLIDING: invalid interval value".into()))?;

            // Parse unit
            let after_num = after_interval_trimmed[num_end..].trim_start();
            let unit_end = after_num
                .find(|c: char| !c.is_ascii_alphabetic())
                .unwrap_or(after_num.len());
            let unit = &after_num[..unit_end];
            let multiplier: u64 = match unit {
                "SECOND" | "SECONDS" => 1,
                "MINUTE" | "MINUTES" => 60,
                "HOUR" | "HOURS" => 3600,
                "DAY" | "DAYS" => 86400,
                _ => {
                    return Err(Error::Schema(format!(
                        "WINDOW SLIDING: unsupported interval unit '{unit}'"
                    )));
                }
            };
            let interval_seconds = n.checked_mul(multiplier).ok_or_else(|| {
                Error::Schema(format!(
                    "WINDOW SLIDING: interval value {n} {unit} overflows"
                ))
            })?;
            if interval_seconds == 0 {
                return Err(Error::Schema(
                    "WINDOW SLIDING: interval must be greater than 0".into(),
                ));
            }

            // Parse BY <column>
            let after_unit = after_num[unit_end..].trim_start();
            if !after_unit.starts_with("BY") {
                return Err(Error::Schema(
                    "WINDOW SLIDING: expected BY <column> after interval".into(),
                ));
            }
            let after_by = after_unit["BY".len()..].trim_start();
            let col_end = after_by
                .find(|c: char| !c.is_ascii_alphanumeric() && c != '_')
                .unwrap_or(after_by.len());
            if col_end == 0 {
                return Err(Error::Schema(
                    "WINDOW SLIDING: expected column name after BY".into(),
                ));
            }
            // Compute the absolute end position of the WINDOW SLIDING clause
            let clause_end_in_upper =
                abs + "WINDOW SLIDING".len() + (rest.len() - after_by.len()) + col_end;
            let time_column = input[clause_end_in_upper - col_end..clause_end_in_upper]
                .trim()
                .to_string();

            // Find MV name by scanning backward for CREATE MATERIALIZED VIEW <name>
            let before = &upper[..abs];
            let mv_name = find_mv_name_before(before, &input[..abs]).ok_or_else(|| {
                Error::Schema(
                    "WINDOW SLIDING: could not determine which MATERIALIZED VIEW this belongs to"
                        .into(),
                )
            })?;

            windows.insert(
                mv_name,
                SlidingWindowDef {
                    interval_seconds,
                    time_column,
                },
            );

            pos = clause_end_in_upper;
        } else {
            result.push_str(&input[pos..]);
            break;
        }
    }

    Ok((result, windows))
}

/// Scan backward from `pos` in the uppercased text to find the most recent
/// `CREATE MATERIALIZED VIEW <name>` and return the name (using original casing).
/// Handles plain identifiers (`my_view`), dotted names (`schema.view`),
/// and quoted identifiers (`"My View"`).
fn find_mv_name_before(upper_before: &str, orig_before: &str) -> Option<String> {
    let keyword = "CREATE MATERIALIZED VIEW";
    let idx = upper_before.rfind(keyword)?;
    let after_keyword = &orig_before[idx + keyword.len()..];
    let trimmed = after_keyword.trim_start();

    if trimmed.starts_with('"') {
        // Quoted identifier: find closing quote
        let end = trimmed[1..].find('"').map(|i| i + 2)?;
        // Return without quotes to match sqlparser's ObjectName.to_string()
        Some(trimmed[1..end - 1].to_string())
    } else {
        // Unquoted: allow alphanumeric, underscore, and dot (for schema.name)
        let name_end = trimmed
            .find(|c: char| !c.is_ascii_alphanumeric() && c != '_' && c != '.')
            .unwrap_or(trimmed.len());
        if name_end == 0 {
            return None;
        }
        Some(trimmed[..name_end].to_string())
    }
}

fn map_column_type(dt: &sql::DataType, col_name: &sql::Ident) -> Result<ColumnType, Error> {
    match dt {
        sql::DataType::UInt64 => Ok(ColumnType::UInt64),
        sql::DataType::UInt256 => Ok(ColumnType::Uint256),
        sql::DataType::Int64 | sql::DataType::BigInt(_) => Ok(ColumnType::Int64),
        sql::DataType::Float64 | sql::DataType::Double(_) => Ok(ColumnType::Float64),
        sql::DataType::Boolean => Ok(ColumnType::Boolean),
        sql::DataType::String(_) | sql::DataType::Varchar(_) | sql::DataType::Text => {
            Ok(ColumnType::String)
        }
        sql::DataType::Datetime(_) | sql::DataType::Timestamp(_, _) => Ok(ColumnType::DateTime),
        sql::DataType::Bytea | sql::DataType::Varbinary(_) | sql::DataType::Blob(_) => {
            Ok(ColumnType::Bytes)
        }
        sql::DataType::Custom(name, _) => match name.to_string().to_lowercase().as_str() {
            "uint64" => Ok(ColumnType::UInt64),
            "int64" => Ok(ColumnType::Int64),
            "float64" => Ok(ColumnType::Float64),
            "uint256" => Ok(ColumnType::Uint256),
            "string" => Ok(ColumnType::String),
            "datetime" => Ok(ColumnType::DateTime),
            "boolean" => Ok(ColumnType::Boolean),
            "bytes" => Ok(ColumnType::Bytes),
            "base58" => Ok(ColumnType::Base58),
            "json" | "jsonb" => Ok(ColumnType::JSON),
            other => Err(Error::Schema(format!(
                "unknown type '{other}' for column '{col_name}'"
            ))),
        },
        _ => Err(Error::Schema(format!(
            "unsupported SQL type '{dt}' for column '{col_name}'"
        ))),
    }
}

fn parse_create_mv(name: &sql::ObjectName, query: &sql::Query) -> Result<MVDef, Error> {
    let view_name = name.to_string();

    // Each field, so that a clause of a new sqlparser version cannot pass
    // unread.
    let sql::Query {
        with,
        body,
        order_by,
        limit,
        limit_by,
        offset,
        fetch,
        locks,
        for_clause,
        settings,
        format_clause,
    } = query;
    let select = match body.as_ref() {
        sql::SetExpr::Select(s) => s.as_ref(),
        _ => {
            return Err(Error::Schema(format!(
                "MV '{view_name}': only simple SELECT supported"
            )));
        }
    };
    let sql::Select {
        select_token: _,
        distinct,
        top,
        top_before_distinct: _,
        projection,
        into,
        from,
        lateral_views,
        prewhere,
        selection,
        group_by,
        cluster_by,
        distribute_by,
        sort_by,
        having,
        named_window,
        qualify,
        window_before_qualify: _,
        value_table_mode,
        connect_by,
        flavor: _,
    } = select;

    let joins = from.len() > 1 || from.iter().any(|f| !f.joins.is_empty());
    let clusters = !cluster_by.is_empty() || !distribute_by.is_empty();
    let limits = limit.is_some() || !limit_by.is_empty() || offset.is_some() || fetch.is_some();
    let refused = [
        (with.is_some(), "WITH"),
        (distinct.is_some() || top.is_some(), "DISTINCT"),
        (into.is_some(), "INTO"),
        (joins || !lateral_views.is_empty(), "JOIN"),
        (prewhere.is_some() || selection.is_some(), "WHERE"),
        (having.is_some() || qualify.is_some(), "HAVING"),
        (order_by.is_some() || !sort_by.is_empty(), "ORDER BY"),
        (clusters, "CLUSTER BY"),
        (limits, "LIMIT"),
        (!named_window.is_empty(), "WINDOW"),
        (value_table_mode.is_some(), "SELECT AS"),
        (connect_by.is_some(), "CONNECT BY"),
        (!locks.is_empty() || for_clause.is_some(), "FOR"),
        (settings.is_some() || format_clause.is_some(), "SETTINGS"),
    ];
    if let Some((_, clause)) = refused.iter().find(|(present, _)| *present) {
        return Err(Error::Schema(format!(
            "MV '{view_name}': {clause} is not supported in a view"
        )));
    }

    // Source table (FROM clause)
    let source = match from.first() {
        Some(sql::TableWithJoins { relation, .. }) => match relation {
            sql::TableFactor::Table {
                name,
                alias,
                args,
                with_hints,
                version,
                with_ordinality,
                partitions,
                json_path,
                sample,
                index_hints,
            } => {
                let hints = !with_hints.is_empty() || !index_hints.is_empty();
                let modifiers = [
                    (alias.is_some(), "a table alias"),
                    (args.is_some(), "a table function"),
                    (sample.is_some(), "TABLESAMPLE"),
                    (version.is_some(), "a table version"),
                    (hints, "a table hint"),
                    (*with_ordinality, "WITH ORDINALITY"),
                    (!partitions.is_empty(), "PARTITION"),
                    (json_path.is_some(), "a JSON path"),
                ];
                if let Some((_, modifier)) = modifiers.iter().find(|(present, _)| *present) {
                    return Err(Error::Schema(format!(
                        "MV '{view_name}': {modifier} in FROM is not supported"
                    )));
                }
                name.to_string()
            }
            _ => {
                return Err(Error::Schema(format!(
                    "MV '{view_name}': unsupported FROM clause"
                )));
            }
        },
        None => {
            return Err(Error::Schema(format!(
                "MV '{view_name}': missing FROM clause"
            )));
        }
    };

    // SELECT items
    let mut items = Vec::new();
    for proj in projection {
        match proj {
            sql::SelectItem::UnnamedExpr(expr) => {
                items.push(parse_select_expr(expr, None, &view_name)?);
            }
            sql::SelectItem::ExprWithAlias { expr, alias } => {
                items.push(parse_select_expr(
                    expr,
                    Some(alias.value.clone()),
                    &view_name,
                )?);
            }
            _ => {
                return Err(Error::Schema(format!(
                    "MV '{view_name}': unsupported select item"
                )));
            }
        }
    }

    // GROUP BY
    let group_by = match group_by {
        sql::GroupByExpr::Expressions(_, modifiers) if !modifiers.is_empty() => {
            return Err(Error::Schema(format!(
                "MV '{view_name}': a GROUP BY modifier is not supported"
            )));
        }
        sql::GroupByExpr::Expressions(exprs, _) => exprs
            .iter()
            .map(|e| expr_to_column_name(e))
            .collect::<Result<Vec<_>, _>>()?,
        sql::GroupByExpr::All(_) => {
            return Err(Error::Schema(format!(
                "MV '{view_name}': GROUP BY ALL not supported"
            )));
        }
    };

    Ok(MVDef {
        name: view_name,
        source,
        select: items,
        group_by,
        sliding_window: None,
    })
}

fn parse_select_expr(
    expr: &sql::Expr,
    alias: Option<String>,
    view_name: &str,
) -> Result<SelectItem, Error> {
    match expr {
        sql::Expr::Identifier(ident) => Ok(SelectItem {
            expr: SelectExpr::Column(ident.value.clone()),
            alias,
        }),
        sql::Expr::Function(func) => {
            let func_name = func.name.to_string().to_lowercase();
            if let Some(modifier) = function_modifier(func) {
                return Err(Error::Schema(format!(
                    "MV '{view_name}': {modifier} in {func_name}() is not supported"
                )));
            }

            // Check for toStartOfInterval(col, INTERVAL N UNIT)
            if func_name == "tostartofinterval" {
                return parse_window_func(func, alias, view_name);
            }

            let agg = match func_name.as_str() {
                "sum" => AggFunc::Sum,
                "count" => AggFunc::Count,
                "min" => AggFunc::Min,
                "max" => AggFunc::Max,
                "avg" => AggFunc::Avg,
                "first" => AggFunc::First,
                "last" => AggFunc::Last,
                _ => {
                    // sqlparser may parse bare column names as zero-arg functions
                    // if they look like identifiers followed by no parens
                    if matches!(&func.args, sql::FunctionArguments::None) {
                        return Ok(SelectItem {
                            expr: SelectExpr::Column(func_name),
                            alias,
                        });
                    }
                    return Err(Error::Schema(format!(
                        "MV '{view_name}': unknown function '{func_name}'"
                    )));
                }
            };

            let col = match &func.args {
                sql::FunctionArguments::List(arg_list) => {
                    if arg_list.args.is_empty() {
                        None // count()
                    } else if arg_list.args.len() == 1 {
                        match &arg_list.args[0] {
                            sql::FunctionArg::Unnamed(sql::FunctionArgExpr::Expr(
                                sql::Expr::Identifier(ident),
                            )) => Some(ident.value.clone()),
                            _ => {
                                return Err(Error::Schema(format!(
                                    "MV '{view_name}': unsupported argument to {func_name}()"
                                )));
                            }
                        }
                    } else {
                        return Err(Error::Schema(format!(
                            "MV '{view_name}': {func_name}() takes 0 or 1 argument"
                        )));
                    }
                }
                sql::FunctionArguments::None => None,
                _ => {
                    return Err(Error::Schema(format!(
                        "MV '{view_name}': unsupported arguments to {func_name}()"
                    )));
                }
            };

            Ok(SelectItem {
                expr: SelectExpr::Agg(agg, col),
                alias,
            })
        }
        _ => Err(Error::Schema(format!(
            "MV '{view_name}': unsupported expression '{expr}'"
        ))),
    }
}

/// The first modifier of a function call, which no view function takes.
fn function_modifier(func: &sql::Function) -> Option<&'static str> {
    // Each field, as for the query.
    let sql::Function {
        name: _,
        uses_odbc_syntax,
        parameters,
        args,
        filter,
        null_treatment,
        over,
        within_group,
    } = func;
    let (distinct, clause) = match args {
        sql::FunctionArguments::List(list) => {
            let distinct = list.duplicate_treatment == Some(sql::DuplicateTreatment::Distinct);
            let clause = list.clauses.first().map(|clause| match clause {
                sql::FunctionArgumentClause::OrderBy(_) => "ORDER BY",
                _ => "a clause after the arguments",
            });
            (distinct, clause)
        }
        _ => (false, None),
    };
    let parametric = !matches!(parameters, sql::FunctionArguments::None);

    let modifiers = [
        (distinct, "DISTINCT"),
        (filter.is_some(), "FILTER"),
        (over.is_some(), "OVER"),
        (!within_group.is_empty(), "WITHIN GROUP"),
        (null_treatment.is_some(), "IGNORE NULLS"),
        (parametric, "a parameter list"),
        (*uses_odbc_syntax, "the ODBC syntax"),
    ];
    let modifier = modifiers.iter().find(|(present, _)| *present);
    modifier.map(|(_, name)| *name).or(clause)
}

fn parse_window_func(
    func: &sql::Function,
    alias: Option<String>,
    view_name: &str,
) -> Result<SelectItem, Error> {
    let args = match &func.args {
        sql::FunctionArguments::List(list) => &list.args,
        _ => {
            return Err(Error::Schema(format!(
                "MV '{view_name}': toStartOfInterval() requires arguments"
            )));
        }
    };

    if args.len() != 2 {
        return Err(Error::Schema(format!(
            "MV '{view_name}': toStartOfInterval() requires exactly 2 arguments"
        )));
    }

    let column = match &args[0] {
        sql::FunctionArg::Unnamed(sql::FunctionArgExpr::Expr(sql::Expr::Identifier(ident))) => {
            ident.value.clone()
        }
        _ => {
            return Err(Error::Schema(format!(
                "MV '{view_name}': toStartOfInterval() first arg must be a column"
            )));
        }
    };

    let interval_seconds = match &args[1] {
        sql::FunctionArg::Unnamed(sql::FunctionArgExpr::Expr(sql::Expr::Interval(iv))) => {
            parse_interval(iv, view_name)?
        }
        _ => {
            return Err(Error::Schema(format!(
                "MV '{view_name}': toStartOfInterval() second arg must be an INTERVAL"
            )));
        }
    };

    Ok(SelectItem {
        expr: SelectExpr::WindowFunc {
            column,
            interval_seconds,
        },
        alias,
    })
}

fn parse_interval(iv: &sql::Interval, view_name: &str) -> Result<u64, Error> {
    let value_str = iv.value.to_string();
    let n: u64 = value_str.trim().parse().map_err(|_| {
        Error::Schema(format!(
            "MV '{view_name}': invalid interval value '{value_str}'"
        ))
    })?;

    let unit = iv
        .leading_field
        .as_ref()
        .ok_or_else(|| Error::Schema(format!("MV '{view_name}': interval missing unit")))?;

    let multiplier: u64 = match unit {
        sql::DateTimeField::Second => 1,
        sql::DateTimeField::Minute => 60,
        sql::DateTimeField::Hour => 3600,
        sql::DateTimeField::Day => 86400,
        _ => {
            return Err(Error::Schema(format!(
                "MV '{view_name}': unsupported interval unit '{unit}'"
            )));
        }
    };

    n.checked_mul(multiplier)
        .ok_or_else(|| Error::Schema(format!("MV '{view_name}': interval {n} {unit} overflows")))
}

fn expr_to_column_name(expr: &sql::Expr) -> Result<String, Error> {
    match expr {
        sql::Expr::Identifier(ident) => Ok(ident.value.clone()),
        // sqlparser may parse some identifiers (like `user`) as functions with no args
        sql::Expr::Function(func)
            if matches!(&func.args, sql::FunctionArguments::None)
                && function_modifier(func).is_none() =>
        {
            Ok(func.name.to_string())
        }
        _ => Err(Error::Schema(format!(
            "GROUP BY expression must be a column name, got '{expr}'"
        ))),
    }
}

// --- Module parser (hand-written, custom syntax) ---

fn extract_modules(input: &str) -> Result<(String, Vec<ModuleDef>), Error> {
    let mut sql_parts = String::new();
    let mut modules = Vec::new();
    let mut rest = input;

    while let Some(pos) = find_create_module(rest) {
        sql_parts.push_str(&rest[..pos]);
        let after = &rest[pos..];
        let (module, consumed) = parse_module_block(after)?;
        modules.push(module);
        rest = &after[consumed..];
    }
    sql_parts.push_str(rest);

    Ok((sql_parts, modules))
}

fn find_create_module(input: &str) -> Option<usize> {
    let upper = input.to_ascii_uppercase();
    let mut search_from = 0;
    while let Some(pos) = upper[search_from..].find("CREATE MODULE") {
        let abs_pos = search_from + pos;
        if abs_pos == 0
            || input.as_bytes()[abs_pos - 1].is_ascii_whitespace()
            || input.as_bytes()[abs_pos - 1] == b';'
        {
            return Some(abs_pos);
        }
        search_from = abs_pos + 1;
    }
    None
}

fn parse_module_block(input: &str) -> Result<(ModuleDef, usize), Error> {
    let bytes = input.as_bytes();
    let len = bytes.len();
    let mut pos = 0;

    // Skip "CREATE MODULE"
    pos = skip_keyword(input, pos, "CREATE")?;
    pos = skip_ws(input, pos);
    pos = skip_keyword(input, pos, "MODULE")?;
    pos = skip_ws(input, pos);

    // Name
    let (name, new_pos) = read_identifier(input, pos)?;
    pos = skip_ws(input, new_pos);

    // Optional "LANGUAGE LUA" (only Lua is supported)
    let upper_here = input[pos..].to_ascii_uppercase();
    if upper_here.starts_with("LANGUAGE") {
        pos = skip_keyword(input, pos, "LANGUAGE")?;
        pos = skip_ws(input, pos);
        pos = skip_keyword(input, pos, "LUA")?;
        pos = skip_ws(input, pos);
    }

    // Optional "AS"
    let upper_here = input[pos..].to_ascii_uppercase();
    if upper_here.starts_with("AS") {
        pos = skip_keyword(input, pos, "AS")?;
        pos = skip_ws(input, pos);
    }

    // Read $$...$$ block
    let (script, new_pos) = read_dollar_quoted(input, pos)?;
    pos = new_pos;

    // Skip optional trailing semicolon
    pos = skip_ws(input, pos);
    if pos < len && bytes[pos] == b';' {
        pos += 1;
    }

    Ok((ModuleDef { name, script }, pos))
}

// --- Reducer parser (hand-written, custom syntax) ---

fn extract_reducers(input: &str) -> Result<(String, Vec<ReducerDef>), Error> {
    let mut sql_parts = String::new();
    let mut reducers = Vec::new();
    let mut rest = input;

    while let Some(pos) = find_create_reducer(rest) {
        sql_parts.push_str(&rest[..pos]);
        let after = &rest[pos..];
        let (reducer, consumed) = parse_reducer_block(after)?;
        reducers.push(reducer);
        rest = &after[consumed..];
    }
    sql_parts.push_str(rest);

    Ok((sql_parts, reducers))
}

fn find_create_reducer(input: &str) -> Option<usize> {
    let upper = input.to_ascii_uppercase();
    let mut search_from = 0;
    while let Some(pos) = upper[search_from..].find("CREATE REDUCER") {
        let abs_pos = search_from + pos;
        // Make sure it's not inside a string or comment
        if abs_pos == 0
            || input.as_bytes()[abs_pos - 1].is_ascii_whitespace()
            || input.as_bytes()[abs_pos - 1] == b';'
        {
            return Some(abs_pos);
        }
        search_from = abs_pos + 1;
    }
    None
}

fn parse_reducer_block(input: &str) -> Result<(ReducerDef, usize), Error> {
    let mut pos = 0;
    let bytes = input.as_bytes();
    let len = bytes.len();

    // Skip "CREATE REDUCER"
    pos = skip_keyword(input, pos, "CREATE")?;
    pos = skip_ws(input, pos);
    pos = skip_keyword(input, pos, "REDUCER")?;
    pos = skip_ws(input, pos);

    // Name
    let (name, new_pos) = read_identifier(input, pos)?;
    pos = skip_ws(input, new_pos);

    // SOURCE
    pos = skip_keyword(input, pos, "SOURCE")?;
    pos = skip_ws(input, pos);
    let (source, new_pos) = read_identifier(input, pos)?;
    pos = skip_ws(input, new_pos);

    // GROUP BY; without it the reducer has one group.
    let mut group_by = Vec::new();
    if let Ok(after) = skip_keyword(input, pos, "GROUP") {
        pos = skip_ws(input, after);
        pos = skip_keyword(input, pos, "BY")?;
        pos = skip_ws(input, pos);
        let (columns, new_pos) = read_identifier_list(input, pos)?;
        group_by = columns;
        pos = skip_ws(input, new_pos);
    }

    // STATE (...)
    pos = skip_keyword(input, pos, "STATE")?;
    pos = skip_ws(input, pos);
    let (state, new_pos) = parse_state_block(input, pos)?;
    pos = skip_ws(input, new_pos);

    // Optional REQUIRE clause (before body)
    let mut requires = Vec::new();
    let upper_here = input[pos..].to_ascii_uppercase();
    if upper_here.starts_with("REQUIRE") {
        pos = skip_keyword(input, pos, "REQUIRE")?;
        pos = skip_ws(input, pos);
        let (req_list, new_pos) = read_identifier_list(input, pos)?;
        requires = req_list;
        pos = skip_ws(input, new_pos);
    }

    let (mut body, new_pos) = parse_reducer_body(input, pos, &name)?;
    pos = new_pos;

    // Fill in External id with reducer name if empty
    if let ReducerBody::External { ref mut id } = body {
        if id.is_empty() {
            *id = name.clone();
        }
    }

    pos = skip_ws(input, pos);
    if pos < len && bytes[pos] == b';' {
        pos += 1;
    }

    Ok((
        ReducerDef {
            name,
            source,
            group_by,
            state,
            requires,
            body,
        },
        pos,
    ))
}

fn parse_state_block(input: &str, start: usize) -> Result<(Vec<StateField>, usize), Error> {
    let bytes = input.as_bytes();
    if start >= bytes.len() || bytes[start] != b'(' {
        return Err(Error::Schema("expected '(' after STATE".into()));
    }
    let mut pos = start + 1;
    let mut fields = Vec::new();

    loop {
        pos = skip_ws(input, pos);
        if pos < bytes.len() && bytes[pos] == b')' {
            pos += 1;
            break;
        }
        if !fields.is_empty() {
            if pos < bytes.len() && bytes[pos] == b',' {
                pos += 1;
                pos = skip_ws(input, pos);
            }
        }

        // field_name Type DEFAULT value
        let (field_name, new_pos) = read_identifier(input, pos)?;
        pos = skip_ws(input, new_pos);

        let (type_name, new_pos) = read_identifier(input, pos)?;
        pos = skip_ws(input, new_pos);

        if type_name.eq_ignore_ascii_case("map") {
            let (column_type, new_pos) = parse_map_type(input, pos, &field_name)?;
            pos = skip_ws(input, new_pos);
            if skip_keyword(input, pos, "DEFAULT").is_ok() {
                return Err(Error::Schema(format!(
                    "state field '{field_name}': a Map field starts with no entries and \
                     takes no DEFAULT"
                )));
            }
            fields.push(StateField {
                name: field_name,
                column_type,
                default: String::new(),
                map: true,
            });
            continue;
        }
        let column_type = str_to_column_type(&type_name)?;

        pos = skip_keyword(input, pos, "DEFAULT")?;
        pos = skip_ws(input, pos);

        let (default, new_pos) = read_token(input, pos)?;
        pos = new_pos;

        fields.push(StateField {
            name: field_name,
            column_type,
            default,
            map: false,
        });
    }

    Ok((fields, pos))
}

/// `(String, ValueType)` after `Map`: the type of the values.
fn parse_map_type(input: &str, start: usize, field: &str) -> Result<(ColumnType, usize), Error> {
    let bytes = input.as_bytes();
    let expect = |pos: usize, c: u8| -> Result<usize, Error> {
        if bytes.get(pos) == Some(&c) {
            return Ok(skip_ws(input, pos + 1));
        }
        Err(Error::Schema(format!(
            "state field '{field}': expected Map(String, Type), got: '{}'",
            snippet(input, start, 30)
        )))
    };

    let pos = expect(start, b'(')?;
    let (key, pos) = read_identifier(input, pos)?;
    let pos = expect(skip_ws(input, pos), b',')?;
    let (value, pos) = read_identifier(input, pos)?;
    let pos = expect(skip_ws(input, pos), b')')?;

    if str_to_column_type(&key)? != ColumnType::String {
        return Err(Error::Schema(format!(
            "state field '{field}': the keys of a Map are String, not {key}"
        )));
    }
    Ok((str_to_column_type(&value)?, pos))
}

fn parse_reducer_body(
    input: &str,
    start: usize,
    reducer: &str,
) -> Result<(ReducerBody, usize), Error> {
    let upper = input[start..].to_ascii_uppercase();
    if upper.starts_with("LANGUAGE") {
        // Check for LANGUAGE EXTERNAL vs LANGUAGE LUA
        let after_lang = upper["LANGUAGE".len()..].trim_start();
        if after_lang.starts_with("EXTERNAL") {
            parse_external_body(input, start)
        } else {
            parse_lua_body(input, start)
        }
    } else if upper.starts_with("WHEN") || upper.starts_with("ALWAYS") {
        Err(Error::Schema(format!(
            "reducer '{reducer}': event rules (WHEN ... THEN) are not supported; \
             write the body in Lua: LANGUAGE lua PROCESS $$ ... $$"
        )))
    } else {
        Err(Error::Schema(format!(
            "expected LANGUAGE lua or LANGUAGE EXTERNAL after STATE, got: '{}'",
            snippet(input, start, 30)
        )))
    }
}

fn parse_external_body(input: &str, start: usize) -> Result<(ReducerBody, usize), Error> {
    let mut pos = start;
    pos = skip_keyword(input, pos, "LANGUAGE")?;
    pos = skip_ws(input, pos);
    pos = skip_keyword(input, pos, "EXTERNAL")?;
    pos = skip_ws(input, pos);

    // The id defaults to empty — filled with the reducer name post-parse
    Ok((ReducerBody::External { id: String::new() }, pos))
}

#[allow(unused_variables)]
fn parse_lua_body(input: &str, start: usize) -> Result<(ReducerBody, usize), Error> {
    let mut pos = start;
    pos = skip_keyword(input, pos, "LANGUAGE")?;
    pos = skip_ws(input, pos);
    pos = skip_keyword(input, pos, "LUA")?;
    pos = skip_ws(input, pos);
    pos = skip_keyword(input, pos, "PROCESS")?;
    pos = skip_ws(input, pos);

    // Read $$...$$ block
    let (script, new_pos) = read_dollar_quoted(input, pos)?;
    #[cfg(not(feature = "lua"))]
    return Err(Error::Schema(
        "Lua reducers require the 'lua' feature. Use LANGUAGE EXTERNAL instead.".into(),
    ));
    #[cfg(feature = "lua")]
    Ok((ReducerBody::Lua { script }, new_pos))
}

// --- Utility functions ---

/// Up to `chars` characters of `input` from byte `pos`, for an error message.
fn snippet(input: &str, pos: usize, chars: usize) -> &str {
    let rest = input.get(pos..).unwrap_or_default();
    let end = rest
        .char_indices()
        .nth(chars)
        .map_or(rest.len(), |(i, _)| i);
    &rest[..end]
}

fn skip_ws(input: &str, pos: usize) -> usize {
    let bytes = input.as_bytes();
    let mut p = pos;
    while p < bytes.len() {
        if bytes[p].is_ascii_whitespace() {
            p += 1;
        } else if p + 1 < bytes.len() && bytes[p] == b'-' && bytes[p + 1] == b'-' {
            // Line comment
            while p < bytes.len() && bytes[p] != b'\n' {
                p += 1;
            }
        } else {
            break;
        }
    }
    p
}

fn skip_keyword(input: &str, pos: usize, keyword: &str) -> Result<usize, Error> {
    let remaining = &input[pos..];
    let upper = remaining.to_ascii_uppercase();
    if upper.starts_with(keyword) {
        let after = pos + keyword.len();
        if after >= input.len() || !input.as_bytes()[after].is_ascii_alphanumeric() {
            return Ok(after);
        }
    }
    Err(Error::Schema(format!(
        "expected '{keyword}' at position {pos}, got: '{}'",
        snippet(input, pos, 20)
    )))
}

fn read_identifier(input: &str, pos: usize) -> Result<(String, usize), Error> {
    let bytes = input.as_bytes();
    let start = pos;
    let mut p = pos;
    while p < bytes.len() && (bytes[p].is_ascii_alphanumeric() || bytes[p] == b'_') {
        p += 1;
    }
    if p == start {
        return Err(Error::Schema(format!(
            "expected identifier at position {pos}, got: '{}'",
            snippet(input, pos, 10)
        )));
    }
    Ok((input[start..p].to_string(), p))
}

fn read_identifier_list(input: &str, pos: usize) -> Result<(Vec<String>, usize), Error> {
    let mut items = Vec::new();
    let mut p = pos;

    loop {
        p = skip_ws(input, p);
        let (name, new_pos) = read_identifier(input, p)?;
        items.push(name);
        p = skip_ws(input, new_pos);

        if p < input.len() && input.as_bytes()[p] == b',' {
            p += 1;
        } else {
            break;
        }
    }

    Ok((items, p))
}

fn read_token(input: &str, pos: usize) -> Result<(String, usize), Error> {
    let bytes = input.as_bytes();
    let start = pos;
    let mut p = pos;

    // Handle quoted strings (with SQL doubled-quote escaping, e.g. 'it''s')
    if p < bytes.len() && (bytes[p] == b'\'' || bytes[p] == b'"') {
        let quote = bytes[p];
        p += 1;
        while p < bytes.len() {
            if bytes[p] == quote {
                // Check for doubled-quote escape (e.g. '' inside a '-quoted string)
                if p + 1 < bytes.len() && bytes[p + 1] == quote {
                    p += 2; // skip both quotes
                } else {
                    p += 1; // closing quote
                    break;
                }
            } else {
                p += 1;
            }
        }
        return Ok((input[start..p].to_string(), p));
    }

    while p < bytes.len()
        && !bytes[p].is_ascii_whitespace()
        && bytes[p] != b','
        && bytes[p] != b')'
        && bytes[p] != b';'
    {
        p += 1;
    }
    if p == start {
        return Err(Error::Schema(format!("expected token at position {pos}")));
    }
    Ok((input[start..p].to_string(), p))
}

fn read_dollar_quoted(input: &str, pos: usize) -> Result<(String, usize), Error> {
    if !input[pos..].starts_with("$$") {
        return Err(Error::Schema(
            "expected '$$' to start dollar-quoted block".into(),
        ));
    }
    let start = pos + 2;
    let end = input[start..]
        .find("$$")
        .ok_or_else(|| Error::Schema("unterminated $$...$$ block".into()))?
        + start;
    Ok((input[start..end].to_string(), end + 2))
}

fn str_to_column_type(s: &str) -> Result<ColumnType, Error> {
    match s.to_lowercase().as_str() {
        "uint64" => Ok(ColumnType::UInt64),
        "int64" => Ok(ColumnType::Int64),
        "float64" => Ok(ColumnType::Float64),
        "uint256" => Ok(ColumnType::Uint256),
        "string" => Ok(ColumnType::String),
        "datetime" => Ok(ColumnType::DateTime),
        "boolean" => Ok(ColumnType::Boolean),
        "bytes" => Ok(ColumnType::Bytes),
        "base58" => Ok(ColumnType::Base58),
        "json" | "jsonb" => Ok(ColumnType::JSON),
        _ => Err(Error::Schema(format!("unknown type: '{s}'"))),
    }
}

// --- Schema validation ---

/// Err if two of `names` are equal.
fn unique<'a>(
    names: impl Iterator<Item = &'a String>,
    owner: &str,
    kind: &str,
) -> Result<(), Error> {
    let mut seen = std::collections::HashSet::new();
    for name in names {
        if !seen.insert(name) {
            return Err(Error::Schema(format!(
                "{owner}two {kind}s are named '{name}'"
            )));
        }
    }
    Ok(())
}

/// A view groups by its key items, the items that are not aggregates. Its
/// GROUP BY names each of them once, by its output name or, for a column,
/// by the column.
fn check_view_keys(mv: &MVDef) -> Result<(), Error> {
    let keys: Vec<&SelectItem> = mv
        .select
        .iter()
        .filter(|item| !matches!(item.expr, SelectExpr::Agg(..)))
        .collect();
    let reads =
        |key: &SelectItem, column: &str| matches!(&key.expr, SelectExpr::Column(c) if c == column);

    let mut named = vec![false; keys.len()];
    for entry in &mv.group_by {
        // As in SQL, a source column wins over an alias. It names each key
        // item that reads it.
        let mut items: Vec<usize> = (0..keys.len()).filter(|&i| reads(keys[i], entry)).collect();
        if items.is_empty() {
            items.extend(keys.iter().position(|key| key.output_name() == *entry));
        }
        if items.is_empty() {
            return Err(Error::Schema(format!(
                "MV '{}': GROUP BY '{entry}' is not a key item of the select list",
                mv.name
            )));
        }

        for i in items {
            if std::mem::replace(&mut named[i], true) {
                return Err(Error::Schema(format!(
                    "MV '{}': GROUP BY names the key item '{}' twice",
                    mv.name,
                    keys[i].output_name()
                )));
            }
        }
    }

    match named.iter().position(|n| !n) {
        Some(i) => Err(Error::Schema(format!(
            "MV '{}': the key item '{}' is not in GROUP BY",
            mv.name,
            keys[i].output_name()
        ))),
        None => Ok(()),
    }
}

/// `sum`, `min`, `max` and `avg` read only numbers. The type of a column of
/// a reducer is known for its state fields and the group-by columns of its
/// source table; for its other emit columns only the rows tell.
fn check_numeric_aggregates(schema: &Schema, mv: &MVDef) -> Result<(), Error> {
    let table_type = |table: &str, column: &str| {
        let table = schema.tables.iter().find(|t| t.name == table)?;
        let column = table.columns.iter().find(|c| c.name == column)?;
        Some(column.column_type.clone())
    };
    let column_type = |column: &str| {
        if let Some(column_type) = table_type(&mv.source, column) {
            return Some(column_type);
        }
        let reducer = schema.reducers.iter().find(|r| r.name == mv.source)?;
        let field = reducer.state.iter().find(|f| f.name == column && !f.map);
        match field {
            Some(field) => Some(field.column_type.clone()),
            None if reducer.group_by.iter().any(|c| c == column) => {
                table_type(&reducer.source, column)
            }
            None => None,
        }
    };

    for item in &mv.select {
        let SelectExpr::Agg(func, Some(column)) = &item.expr else {
            continue;
        };
        let numeric = matches!(
            func,
            AggFunc::Sum | AggFunc::Min | AggFunc::Max | AggFunc::Avg
        );
        let no_mode = matches!(
            column_type(column),
            Some(ColumnType::String | ColumnType::Boolean | ColumnType::Bytes)
                | Some(ColumnType::Base58 | ColumnType::JSON)
        );
        if numeric && no_mode {
            return Err(Error::Schema(format!(
                "MV '{}': {}() reads numbers, and column '{column}' holds none",
                mv.name,
                func.name()
            )));
        }
    }
    Ok(())
}

fn validate_schema(schema: &Schema) -> Result<(), Error> {
    let table_names: Vec<&str> = schema.tables.iter().map(|t| t.name.as_str()).collect();
    let reducer_names: Vec<&str> = schema.reducers.iter().map(|r| r.name.as_str()).collect();
    let module_names: Vec<&str> = schema.modules.iter().map(|m| m.name.as_str()).collect();

    // A stored row holds its column count in a u16, and a column position
    // is a u16.
    let max_columns = u16::MAX as usize;
    for table in &schema.tables {
        if table.columns.len() > max_columns {
            return Err(Error::Schema(format!(
                "table '{}': {} columns, at most {max_columns} are supported",
                table.name,
                table.columns.len()
            )));
        }
    }
    for reducer in &schema.reducers {
        if reducer.state.len() > max_columns {
            return Err(Error::Schema(format!(
                "reducer '{}': {} STATE fields, at most {max_columns} are supported",
                reducer.name,
                reducer.state.len()
            )));
        }
    }

    // Validate module name uniqueness
    {
        let mut seen = std::collections::HashSet::new();
        for name in &module_names {
            if !seen.insert(name) {
                return Err(Error::Schema(format!("duplicate module name: '{name}'")));
            }
        }
    }

    // A source names a table or a reducer, and the output names a table or
    // a view: the three share one namespace.
    let objects = schema.tables.iter().map(|t| &t.name);
    let objects = objects
        .chain(schema.reducers.iter().map(|r| &r.name))
        .chain(schema.materialized_views.iter().map(|v| &v.name));
    unique(objects, "", "object")?;
    for table in &schema.tables {
        let owner = format!("table '{}': ", table.name);
        unique(table.columns.iter().map(|c| &c.name), &owner, "column")?;
    }
    for reducer in &schema.reducers {
        let owner = format!("reducer '{}': ", reducer.name);
        unique(reducer.state.iter().map(|f| &f.name), &owner, "state field")?;
    }

    // Validate reducers
    for reducer in &schema.reducers {
        // Validate REQUIRE references
        for req in &reducer.requires {
            if !module_names.contains(&req.as_str()) {
                return Err(Error::Schema(format!(
                    "reducer '{}': REQUIRE references unknown module '{}'",
                    reducer.name, req
                )));
            }
        }
        let source_is_table = table_names.contains(&reducer.source.as_str());
        let source_is_reducer = reducer_names.contains(&reducer.source.as_str());

        if !source_is_table && !source_is_reducer {
            return Err(Error::Schema(format!(
                "reducer '{}': source '{}' is not a known table or reducer",
                reducer.name, reducer.source
            )));
        }

        // Self-cycle check
        if reducer.source == reducer.name {
            return Err(Error::Schema(format!(
                "reducer '{}': cannot source from itself",
                reducer.name
            )));
        }

        // Validate GROUP BY columns against source table (only when source is a table;
        // for chained reducers, output columns are dynamic and validated at runtime)
        if source_is_table {
            let source_table = schema
                .tables
                .iter()
                .find(|t| t.name == reducer.source)
                .unwrap();
            for col in &reducer.group_by {
                if !source_table.columns.iter().any(|c| c.name == *col) {
                    return Err(Error::Schema(format!(
                        "reducer '{}': GROUP BY column '{}' not found in source table '{}'",
                        reducer.name, col, reducer.source
                    )));
                }
            }
        }
    }

    // Detect circular dependencies between reducers
    {
        let reducer_source_map: std::collections::HashMap<&str, &str> = schema
            .reducers
            .iter()
            .filter(|r| reducer_names.contains(&r.source.as_str()))
            .map(|r| (r.name.as_str(), r.source.as_str()))
            .collect();

        for start in reducer_source_map.keys() {
            let mut visited = std::collections::HashSet::new();
            let mut current = *start;
            while let Some(&next) = reducer_source_map.get(current) {
                if !visited.insert(current) {
                    return Err(Error::Schema(format!(
                        "circular dependency detected among reducers involving '{current}'"
                    )));
                }
                current = next;
            }
        }
    }

    // Validate MVs
    for mv in &schema.materialized_views {
        let source_is_table = table_names.contains(&mv.source.as_str());
        let source_is_reducer = reducer_names.contains(&mv.source.as_str());

        if !source_is_table && !source_is_reducer {
            return Err(Error::Schema(format!(
                "MV '{}': source '{}' is not a known table or reducer",
                mv.name, mv.source
            )));
        }

        let agg_count = mv
            .select
            .iter()
            .filter(|item| matches!(item.expr, SelectExpr::Agg(..)))
            .count();
        if agg_count > MAX_MV_AGGREGATES {
            return Err(Error::Schema(format!(
                "MV '{}': {agg_count} aggregate columns, at most {MAX_MV_AGGREGATES} are supported",
                mv.name
            )));
        }

        // A view reads its source rows by position, so a raw table must
        // have each column that it reads.
        if source_is_table {
            let source_table = schema.tables.iter().find(|t| t.name == mv.source).unwrap();
            let read = mv.select.iter().filter_map(|item| match &item.expr {
                SelectExpr::Column(col) | SelectExpr::WindowFunc { column: col, .. } => Some(col),
                SelectExpr::Agg(_, col) => col.as_ref(),
            });
            for col in read {
                if !source_table.columns.iter().any(|c| c.name == *col) {
                    return Err(Error::Schema(format!(
                        "MV '{}': column '{}' not found in source table '{}'",
                        mv.name, col, mv.source
                    )));
                }
            }
        }

        // Validate sliding window time_column exists in source table with a numeric type
        if let Some(ref sw) = mv.sliding_window {
            if source_is_table {
                let source_table = schema.tables.iter().find(|t| t.name == mv.source).unwrap();
                let col = source_table
                    .columns
                    .iter()
                    .find(|c| c.name == sw.time_column);
                match col {
                    None => {
                        return Err(Error::Schema(format!(
                            "MV '{}': WINDOW SLIDING BY column '{}' not found in source table '{}'",
                            mv.name, sw.time_column, mv.source
                        )));
                    }
                    Some(c) => {
                        let valid = matches!(
                            c.column_type,
                            ColumnType::DateTime | ColumnType::UInt64 | ColumnType::Int64
                        );
                        if !valid {
                            return Err(Error::Schema(format!(
                                "MV '{}': WINDOW SLIDING BY column '{}' must be DateTime, UInt64, or Int64 (got {:?})",
                                mv.name, sw.time_column, c.column_type
                            )));
                        }
                    }
                }
            }
        }

        if agg_count == 0 {
            return Err(Error::Schema(format!(
                "MV '{}': a view needs at least one aggregate",
                mv.name
            )));
        }
        let outputs: Vec<String> = mv.select.iter().map(SelectItem::output_name).collect();
        let owner = format!("MV '{}': ", mv.name);
        unique(outputs.iter(), &owner, "output column")?;
        check_view_keys(mv)?;
        check_numeric_aggregates(schema, mv)?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_simple_table() {
        let sql = r#"
            CREATE TABLE swaps (
                block_number UInt64,
                block_time   DateTime,
                user         String,
                amount       Float64
            );
        "#;
        let schema = parse_schema(sql).unwrap();
        assert_eq!(schema.tables.len(), 1);
        let t = &schema.tables[0];
        assert_eq!(t.name, "swaps");
        assert_eq!(t.columns.len(), 4);
        assert_eq!(t.columns[0].name, "block_number");
        assert_eq!(t.columns[0].column_type, ColumnType::UInt64);
        assert_eq!(t.columns[1].column_type, ColumnType::DateTime);
        assert_eq!(t.columns[2].column_type, ColumnType::String);
        assert_eq!(t.columns[3].column_type, ColumnType::Float64);
    }

    #[test]
    fn parse_simple_mv() {
        let sql = r#"
            CREATE TABLE trades (
                block_number UInt64,
                block_time   DateTime,
                pair         String,
                price        Float64,
                amount       Float64
            );

            CREATE MATERIALIZED VIEW candles_5m AS
              SELECT
                pair,
                toStartOfInterval(block_time, INTERVAL 5 MINUTE) AS window_start,
                first(price)  AS open,
                max(price)    AS high,
                min(price)    AS low,
                last(price)   AS close,
                sum(amount)   AS volume,
                count()       AS trade_count
              FROM trades
              GROUP BY pair, window_start;
        "#;
        let schema = parse_schema(sql).unwrap();
        assert_eq!(schema.tables.len(), 1);
        assert_eq!(schema.materialized_views.len(), 1);

        let mv = &schema.materialized_views[0];
        assert_eq!(mv.name, "candles_5m");
        assert_eq!(mv.source, "trades");
        assert_eq!(mv.group_by, vec!["pair", "window_start"]);
        assert_eq!(mv.select.len(), 8);

        // Check window function
        match &mv.select[1].expr {
            SelectExpr::WindowFunc {
                column,
                interval_seconds,
            } => {
                assert_eq!(column, "block_time");
                assert_eq!(*interval_seconds, 300); // 5 minutes
            }
            _ => panic!("expected WindowFunc"),
        }
        assert_eq!(mv.select[1].alias.as_deref(), Some("window_start"));

        // Check aggregations
        match &mv.select[2].expr {
            SelectExpr::Agg(AggFunc::First, Some(col)) => assert_eq!(col, "price"),
            _ => panic!("expected first(price)"),
        }
        match &mv.select[6].expr {
            SelectExpr::Agg(AggFunc::Sum, Some(col)) => assert_eq!(col, "amount"),
            _ => panic!("expected sum(amount)"),
        }
        match &mv.select[7].expr {
            SelectExpr::Agg(AggFunc::Count, None) => {}
            _ => panic!("expected count()"),
        }
    }

    /// ADR-10: event rules are gone, and the error names Lua.
    #[test]
    fn rejects_an_event_rules_body() {
        let reducer = |body: &str| {
            format!(
                "CREATE TABLE trades (block_number UInt64, user String, amount Float64);
                 CREATE REDUCER pnl
                   SOURCE trades
                   GROUP BY user
                   STATE (qty Float64 DEFAULT 0)
                   {body};"
            )
        };

        for body in [
            "WHEN row.amount > 0 THEN SET state.qty = state.qty + row.amount EMIT qty = state.qty",
            "ALWAYS EMIT state.qty AS qty",
        ] {
            let err = parse_schema(&reducer(body)).unwrap_err();
            assert!(matches!(err, Error::Schema(_)), "{body}: {err}");
            let message = err.to_string();
            assert!(message.contains("reducer 'pnl'"), "{message}");
            assert!(message.contains("LANGUAGE lua"), "{message}");
        }
    }

    #[test]
    fn parse_lua_reducer() {
        let sql = r#"
            CREATE TABLE trades (
                block_number UInt64,
                user         String,
                token        String,
                side         String,
                amount       Float64,
                price        Float64
            );

            CREATE REDUCER fifo_tracker
              SOURCE trades
              GROUP BY user, token
              STATE (
                lots     String DEFAULT '[]',
                realized Float64 DEFAULT 0
              )
              LANGUAGE lua
              PROCESS $$
                local lots = json.decode(state.lots)
                if row.side == 'buy' then
                  table.insert(lots, { qty = row.amount, price = row.price })
                end
                state.lots = json.encode(lots)
                emit.position_size = #lots
              $$;
        "#;
        let schema = parse_schema(sql).unwrap();
        assert_eq!(schema.reducers.len(), 1);

        let r = &schema.reducers[0];
        assert_eq!(r.name, "fifo_tracker");
        assert_eq!(r.source, "trades");
        assert_eq!(r.group_by, vec!["user", "token"]);
        assert_eq!(r.state.len(), 2);

        match &r.body {
            ReducerBody::Lua { script } => {
                assert!(script.contains("json.decode"));
                assert!(script.contains("emit.position_size"));
            }
            _ => panic!("expected Lua body"),
        }
    }

    #[test]
    fn validation_rejects_bad_mv_source() {
        let sql = r#"
            CREATE TABLE trades (
                block_number UInt64,
                price Float64
            );

            CREATE MATERIALIZED VIEW bad_mv AS
              SELECT sum(price) AS total
              FROM nonexistent_table
              GROUP BY price;
        "#;
        let err = parse_schema(sql).unwrap_err();
        assert!(err.to_string().contains("nonexistent_table"));
    }

    /// A view reads the rows of a raw table by position: a column that the
    /// table lacks would read as Null.
    #[test]
    fn validation_rejects_mv_column_missing_from_source_table() {
        let mv_sql = |select: &str| {
            format!(
                "CREATE TABLE trades (pair String, ts DateTime, price Float64);
                 CREATE MATERIALIZED VIEW v AS SELECT {select} FROM trades GROUP BY pair, hour;"
            )
        };

        parse_schema(&mv_sql(
            "pair, toStartOfInterval(ts, INTERVAL 1 HOUR) AS hour, sum(price) AS s, count() AS n",
        ))
        .expect("every column is in the table");

        for (select, missing) in [
            ("block_number, sum(price) AS s", "block_number"),
            ("pair, sum(amount) AS s", "amount"),
            (
                "pair, toStartOfInterval(time, INTERVAL 1 HOUR) AS hour",
                "time",
            ),
        ] {
            let err = parse_schema(&mv_sql(select)).unwrap_err();
            assert!(
                err.to_string().contains(&format!(
                    "column '{missing}' not found in source table 'trades'"
                )),
                "{select}: {err}"
            );
        }
    }

    #[test]
    fn validation_rejects_mv_with_too_many_aggregates() {
        // The persisted group state stores the aggregate count in one byte: a
        // wider MV would lose its state at reopen.
        let mv_sql = |aggregates: usize| {
            let columns: Vec<String> = (0..aggregates)
                .map(|i| format!("sum(price) AS s{i}"))
                .collect();
            format!(
                "CREATE TABLE trades (block_number UInt64, pair String, price Float64);
                 CREATE MATERIALIZED VIEW wide AS
                   SELECT pair, {} FROM trades GROUP BY pair;",
                columns.join(", ")
            )
        };

        parse_schema(&mv_sql(MAX_MV_AGGREGATES)).expect("the limit itself is allowed");

        let Err(err) = parse_schema(&mv_sql(MAX_MV_AGGREGATES + 1)) else {
            panic!("an MV with more than {MAX_MV_AGGREGATES} aggregates must be rejected");
        };
        assert!(matches!(err, Error::Schema(_)), "{err}");
        assert!(err.to_string().contains("wide"), "{err}");
    }

    #[test]
    fn validation_rejects_bad_reducer_source() {
        let sql = r#"
            CREATE REDUCER bad_reducer
              SOURCE nonexistent_table
              GROUP BY user
              STATE (qty Float64 DEFAULT 0)
              LANGUAGE lua
              PROCESS $$
                state.qty = state.qty + 1
                emit { count = state.qty }
              $$;
        "#;
        let err = parse_schema(sql).unwrap_err();
        assert!(err.to_string().contains("nonexistent_table"));
    }

    /// A stored row holds its column count, and a column position, in a u16.
    #[test]
    fn validation_rejects_more_columns_than_a_u16_holds() {
        let columns = |n: usize| {
            (0..n)
                .map(|i| ColumnDef {
                    name: format!("c{i}"),
                    column_type: ColumnType::UInt64,
                })
                .collect()
        };
        let schema = |n: usize| Schema {
            tables: vec![TableDef {
                name: "wide".to_string(),
                columns: columns(n),
                virtual_table: false,
            }],
            modules: vec![],
            reducers: vec![],
            materialized_views: vec![],
        };

        validate_schema(&schema(u16::MAX as usize)).unwrap();
        let err = validate_schema(&schema(u16::MAX as usize + 1)).unwrap_err();
        assert!(
            err.to_string().contains("table 'wide': 65536 columns"),
            "{err}"
        );
    }

    #[test]
    fn validation_rejects_bad_group_by_column() {
        let sql = r#"
            CREATE TABLE trades (
                block_number UInt64,
                user String,
                price Float64
            );

            CREATE REDUCER bad_reducer
              SOURCE trades
              GROUP BY user, nonexistent_col
              STATE (qty Float64 DEFAULT 0)
              LANGUAGE lua
              PROCESS $$
                state.qty = state.qty + 1
                emit { count = state.qty }
              $$;
        "#;
        let err = parse_schema(sql).unwrap_err();
        assert!(err.to_string().contains("nonexistent_col"));
    }

    #[test]
    fn parse_full_dex_schema() {
        // RFC Section 11 full example
        let sql = r#"
            CREATE TABLE swaps (
                block_number UInt64,
                block_time   DateTime,
                user         String,
                pool         String,
                token_in     String,
                token_out    String,
                amount_in    Float64,
                amount_out   Float64,
                price        Float64
            );

            CREATE REDUCER pnl_tracker
              SOURCE swaps
              GROUP BY user, token_in
              STATE (
                quantity   Float64 DEFAULT 0,
                cost_basis Float64 DEFAULT 0
              )
              LANGUAGE lua
              PROCESS $$
                local trade_pnl = nil
                if row.amount_in > 0 then
                  local avg_cost = state.cost_basis / state.quantity
                  state.quantity = state.quantity - row.amount_in
                  state.cost_basis = state.cost_basis - row.amount_in * avg_cost
                  trade_pnl = row.amount_in * (row.price - avg_cost)
                elseif row.amount_out > 0 then
                  state.quantity = state.quantity + row.amount_out
                  state.cost_basis = state.cost_basis + row.amount_out * row.price
                  trade_pnl = 0
                end
                local avg_cost = 0
                if state.quantity > 0 then avg_cost = state.cost_basis / state.quantity end
                emit {
                  trade_pnl = trade_pnl,
                  position_size = state.quantity,
                  avg_cost = avg_cost,
                }
              $$;

            CREATE MATERIALIZED VIEW pnl_5m AS
              SELECT
                user, token_in AS token,
                toStartOfInterval(block_time, INTERVAL 5 MINUTE) AS window_start,
                sum(trade_pnl)  AS realized_pnl,
                count()          AS trade_count
              FROM pnl_tracker
              GROUP BY user, token, window_start;

            CREATE MATERIALIZED VIEW positions AS
              SELECT
                user, token_in AS token,
                last(position_size)   AS position_size,
                last(avg_cost)        AS avg_cost,
                sum(trade_pnl)        AS total_realized_pnl
              FROM pnl_tracker
              GROUP BY user, token;

            CREATE MATERIALIZED VIEW volume_5m AS
              SELECT
                pool,
                toStartOfInterval(block_time, INTERVAL 5 MINUTE) AS window_start,
                sum(amount_in)   AS volume_in,
                sum(amount_out)  AS volume_out,
                count()          AS swap_count,
                max(amount_in)   AS max_swap
              FROM swaps
              GROUP BY pool, window_start;
        "#;
        let schema = parse_schema(sql).unwrap();
        assert_eq!(schema.tables.len(), 1);
        assert_eq!(schema.tables[0].name, "swaps");
        assert_eq!(schema.reducers.len(), 1);
        assert_eq!(schema.reducers[0].name, "pnl_tracker");
        assert_eq!(schema.materialized_views.len(), 3);
        assert_eq!(schema.materialized_views[0].name, "pnl_5m");
        assert_eq!(schema.materialized_views[1].name, "positions");
        assert_eq!(schema.materialized_views[2].name, "volume_5m");
    }

    /// Issue #24: read_token must handle SQL doubled-quote escaping.
    /// A Map field has a value type and no DEFAULT, and a reducer without
    /// GROUP BY has no group-by columns.
    #[test]
    fn parse_map_state_field() {
        let sql = r#"
            CREATE TABLE swaps (block_number UInt64, token String, price Float64);
            CREATE REDUCER prices
            SOURCE swaps
            STATE (
                swaps UInt64 DEFAULT 0,
                usdc  Map(String, Float64),
                book  map( string , JSON )
            )
            LANGUAGE lua
            PROCESS $$ state.swaps = state.swaps + 1 $$;
        "#;
        let schema = parse_schema(sql).unwrap();
        let reducer = &schema.reducers[0];
        assert!(reducer.group_by.is_empty());

        let fields: Vec<_> = reducer
            .state
            .iter()
            .map(|f| (f.name.as_str(), f.type_name(), f.default.as_str()))
            .collect();
        assert_eq!(
            fields,
            [
                ("swaps", "UInt64".to_string(), "0"),
                ("usdc", "Map(String, Float64)".to_string(), ""),
                ("book", "Map(String, JSON)".to_string(), ""),
            ]
        );
    }

    #[test]
    fn map_state_field_errors() {
        let reducer = |state: &str| {
            format!(
                "CREATE TABLE t (block_number UInt64, k String);
                 CREATE REDUCER r SOURCE t STATE ({state}) LANGUAGE lua PROCESS $$ $$;"
            )
        };
        let cases = [
            ("m Map(UInt64, Float64)", "the keys of a Map are String, not UInt64"),
            ("m Map(String, Float64) DEFAULT '{}'", "takes no DEFAULT"),
            ("m Map(String)", "expected Map(String, Type)"),
            ("m Map(String, Money)", "unknown type: 'Money'"),
        ];
        for (state, error) in cases {
            let err = parse_schema(&reducer(state)).unwrap_err();
            assert!(err.to_string().contains(error), "{state}: {err}");
        }
    }

    #[test]
    fn read_token_handles_doubled_quotes() {
        let (token, end) = read_token("'it''s fine' rest", 0).unwrap();
        assert_eq!(token, "'it''s fine'");
        assert_eq!(end, 12);
    }

    #[test]
    fn parse_table_with_uint256_and_base58() {
        let sql = r#"
            CREATE TABLE transfers (
                block_number UInt64,
                block_time   DateTime,
                sender       Base58,
                receiver     Base58,
                amount       Uint256,
                memo         String
            );
        "#;
        let schema = parse_schema(sql).unwrap();
        assert_eq!(schema.tables.len(), 1);
        let t = &schema.tables[0];
        assert_eq!(t.name, "transfers");
        assert_eq!(t.columns.len(), 6);
        assert_eq!(t.columns[2].name, "sender");
        assert_eq!(t.columns[2].column_type, ColumnType::Base58);
        assert_eq!(t.columns[3].name, "receiver");
        assert_eq!(t.columns[3].column_type, ColumnType::Base58);
        assert_eq!(t.columns[4].name, "amount");
        assert_eq!(t.columns[4].column_type, ColumnType::Uint256);
    }

    #[test]
    fn parse_reducer_with_uint256_state() {
        let sql = r#"
            CREATE TABLE transfers (
                block_number UInt64,
                sender       Base58,
                receiver     Base58,
                amount       Uint256
            );

            CREATE REDUCER balance_tracker
              SOURCE transfers
              GROUP BY receiver
              STATE (
                total_received Uint256 DEFAULT 0
              )
              LANGUAGE lua
              PROCESS $$
                emit { balance = state.total_received }
              $$;
        "#;
        let schema = parse_schema(sql).unwrap();
        assert_eq!(schema.reducers.len(), 1);
        let r = &schema.reducers[0];
        assert_eq!(r.state[0].name, "total_received");
        assert_eq!(r.state[0].column_type, ColumnType::Uint256);
    }

    #[test]
    fn parse_create_module() {
        let sql = r#"
            CREATE MODULE pricing LANGUAGE LUA AS $$
                local M = {}
                function M.to_usd(amount, decimals)
                    return amount / (10 ^ decimals)
                end
                return M
            $$;

            CREATE TABLE swaps (
                block_number UInt64,
                token        String,
                amount       Float64
            );

            CREATE REDUCER token_stats
              SOURCE swaps
              GROUP BY token
              STATE (
                volume Float64 DEFAULT 0
              )
              REQUIRE pricing
              LANGUAGE lua
              PROCESS $$
                local usd = pricing.to_usd(row.amount, 6)
                state.volume = state.volume + usd
                emit.token = row.token
                emit.volume = state.volume
              $$;
        "#;
        let schema = parse_schema(sql).unwrap();
        assert_eq!(schema.modules.len(), 1);
        assert_eq!(schema.modules[0].name, "pricing");
        assert!(schema.modules[0].script.contains("function M.to_usd"));

        assert_eq!(schema.reducers.len(), 1);
        assert_eq!(schema.reducers[0].requires, vec!["pricing".to_string()]);
    }

    #[test]
    fn parse_module_validation_unknown_require() {
        let sql = r#"
            CREATE TABLE events (
                block_number UInt64,
                value        Float64
            );

            CREATE REDUCER counter
              SOURCE events
              GROUP BY value
              STATE (count Float64 DEFAULT 0)
              REQUIRE nonexistent
              LANGUAGE lua
              PROCESS $$
                state.count = state.count + 1
              $$;
        "#;
        let err = parse_schema(sql).unwrap_err();
        assert!(err.to_string().contains("nonexistent"));
    }

    #[test]
    fn parse_module_duplicate_name() {
        let sql = r#"
            CREATE MODULE utils LANGUAGE LUA AS $$ return {} $$;
            CREATE MODULE utils LANGUAGE LUA AS $$ return {} $$;

            CREATE TABLE t (x UInt64);
        "#;
        let err = parse_schema(sql).unwrap_err();
        assert!(err.to_string().contains("duplicate module"));
    }

    #[test]
    fn parse_reducer_chaining() {
        let sql = r#"
            CREATE TABLE events (
                block_number UInt64,
                user         String,
                amount       Float64
            );

            CREATE REDUCER enriched
              SOURCE events
              GROUP BY user
              STATE (total Float64 DEFAULT 0)
              LANGUAGE lua
              PROCESS $$
                state.total = state.total + row.amount
                emit.user = row.user
                emit.total = state.total
              $$;

            CREATE REDUCER alerts
              SOURCE enriched
              GROUP BY user
              STATE (prev Float64 DEFAULT 0)
              LANGUAGE lua
              PROCESS $$
                if row.total > state.prev * 2 and state.prev > 0 then
                    emit.user = row.user
                    emit.spike = row.total
                end
                state.prev = row.total
              $$;
        "#;
        let schema = parse_schema(sql).unwrap();
        assert_eq!(schema.reducers.len(), 2);
        assert_eq!(schema.reducers[0].source, "events");
        assert_eq!(schema.reducers[1].source, "enriched");
    }

    #[test]
    fn parse_reducer_circular_dependency() {
        let sql = r#"
            CREATE TABLE t (x UInt64);

            CREATE REDUCER a
              SOURCE b
              GROUP BY x
              STATE (v Float64 DEFAULT 0)
              LANGUAGE lua
              PROCESS $$ emit.x = 1 $$;

            CREATE REDUCER b
              SOURCE a
              GROUP BY x
              STATE (v Float64 DEFAULT 0)
              LANGUAGE lua
              PROCESS $$ emit.x = 1 $$;
        "#;
        let err = parse_schema(sql).unwrap_err();
        assert!(err.to_string().contains("circular dependency"));
    }

    #[test]
    fn parse_reducer_self_source() {
        let sql = r#"
            CREATE TABLE t (x UInt64);

            CREATE REDUCER self_ref
              SOURCE self_ref
              GROUP BY x
              STATE (v Float64 DEFAULT 0)
              LANGUAGE lua
              PROCESS $$ emit.x = 1 $$;
        "#;
        let err = parse_schema(sql).unwrap_err();
        assert!(err.to_string().contains("cannot source from itself"));
    }

    // -----------------------------------------------------------------------
    // Sliding window parser tests
    // -----------------------------------------------------------------------

    #[test]
    fn parse_sliding_window_basic() {
        let sql = r#"
            CREATE TABLE trades (
                block_number UInt64,
                block_time   DateTime,
                pair         String,
                volume       Float64
            );

            CREATE MATERIALIZED VIEW volume_1h AS
              SELECT pair, SUM(volume) AS total_volume, COUNT() AS trade_count
              FROM trades
              GROUP BY pair
              WINDOW SLIDING INTERVAL 1 HOUR BY block_time;
        "#;
        let schema = parse_schema(sql).unwrap();
        assert_eq!(schema.materialized_views.len(), 1);
        let mv = &schema.materialized_views[0];
        assert_eq!(mv.name, "volume_1h");
        assert_eq!(mv.group_by, vec!["pair"]);

        let sw = mv
            .sliding_window
            .as_ref()
            .expect("should have sliding window");
        assert_eq!(sw.interval_seconds, 3600);
        assert_eq!(sw.time_column, "block_time");
    }

    #[test]
    fn parse_sliding_window_various_units() {
        let cases = [
            ("INTERVAL 30 SECOND", 30u64),
            ("INTERVAL 5 MINUTE", 300),
            ("INTERVAL 2 HOUR", 7200),
            ("INTERVAL 1 DAY", 86400),
        ];
        for (interval, expected_secs) in cases {
            let sql = format!(
                r#"
                CREATE TABLE t (block_number UInt64, ts DateTime, x Float64);
                CREATE MATERIALIZED VIEW mv AS
                  SELECT SUM(x) AS total FROM t
                  WINDOW SLIDING {interval} BY ts;
                "#
            );
            let schema = parse_schema(&sql).unwrap();
            let sw = schema.materialized_views[0]
                .sliding_window
                .as_ref()
                .expect("should have sliding window");
            assert_eq!(sw.interval_seconds, expected_secs, "failed for {interval}");
            assert_eq!(sw.time_column, "ts");
        }
    }

    #[test]
    fn parse_sliding_window_mixed_mvs() {
        let sql = r#"
            CREATE TABLE trades (
                block_number UInt64,
                block_time   DateTime,
                pair         String,
                volume       Float64,
                price        Float64
            );

            CREATE MATERIALIZED VIEW candles_5m AS
              SELECT
                pair,
                toStartOfInterval(block_time, INTERVAL 5 MINUTE) AS window_start,
                SUM(volume) AS vol
              FROM trades
              GROUP BY pair, window_start;

            CREATE MATERIALIZED VIEW volume_1h AS
              SELECT pair, SUM(volume) AS total
              FROM trades
              GROUP BY pair
              WINDOW SLIDING INTERVAL 1 HOUR BY block_time;

            CREATE MATERIALIZED VIEW totals AS
              SELECT pair, SUM(volume) AS total
              FROM trades
              GROUP BY pair;
        "#;
        let schema = parse_schema(sql).unwrap();
        assert_eq!(schema.materialized_views.len(), 3);

        // candles_5m: no sliding window (tumbling)
        assert!(schema.materialized_views[0].sliding_window.is_none());

        // volume_1h: sliding window
        let sw = schema.materialized_views[1]
            .sliding_window
            .as_ref()
            .expect("volume_1h should have sliding window");
        assert_eq!(sw.interval_seconds, 3600);

        // totals: no sliding window (unbounded)
        assert!(schema.materialized_views[2].sliding_window.is_none());
    }

    #[test]
    fn parse_sliding_window_no_window_backward_compat() {
        let sql = r#"
            CREATE TABLE t (block_number UInt64, x Float64);
            CREATE MATERIALIZED VIEW mv AS
              SELECT SUM(x) AS total FROM t;
        "#;
        let schema = parse_schema(sql).unwrap();
        assert!(schema.materialized_views[0].sliding_window.is_none());
    }

    #[test]
    fn parse_sliding_window_rejects_missing_interval() {
        let sql = r#"
            CREATE TABLE t (block_number UInt64, ts DateTime, x Float64);
            CREATE MATERIALIZED VIEW mv AS
              SELECT SUM(x) AS total FROM t
              WINDOW SLIDING BY ts;
        "#;
        let err = parse_schema(sql).unwrap_err();
        assert!(err.to_string().contains("INTERVAL"));
    }

    #[test]
    fn parse_sliding_window_rejects_zero_interval() {
        let sql = r#"
            CREATE TABLE t (block_number UInt64, ts DateTime, x Float64);
            CREATE MATERIALIZED VIEW mv AS
              SELECT SUM(x) AS total FROM t
              WINDOW SLIDING INTERVAL 0 HOUR BY ts;
        "#;
        let err = parse_schema(sql).unwrap_err();
        assert!(err.to_string().contains("greater than 0"));
    }

    #[test]
    fn parse_sliding_window_rejects_missing_by() {
        let sql = r#"
            CREATE TABLE t (block_number UInt64, ts DateTime, x Float64);
            CREATE MATERIALIZED VIEW mv AS
              SELECT SUM(x) AS total FROM t
              WINDOW SLIDING INTERVAL 1 HOUR;
        "#;
        let err = parse_schema(sql).unwrap_err();
        assert!(err.to_string().contains("BY"));
    }

    #[test]
    fn parse_sliding_window_rejects_bad_time_column() {
        let sql = r#"
            CREATE TABLE t (block_number UInt64, x Float64);
            CREATE MATERIALIZED VIEW mv AS
              SELECT SUM(x) AS total FROM t
              WINDOW SLIDING INTERVAL 1 HOUR BY nonexistent;
        "#;
        let err = parse_schema(sql).unwrap_err();
        assert!(err.to_string().contains("nonexistent"));
    }

    #[test]
    fn parse_sliding_window_rejects_non_datetime_column() {
        let sql = r#"
            CREATE TABLE t (block_number UInt64, name String, x Float64);
            CREATE MATERIALIZED VIEW mv AS
              SELECT SUM(x) AS total FROM t
              WINDOW SLIDING INTERVAL 1 HOUR BY name;
        "#;
        let err = parse_schema(sql).unwrap_err();
        assert!(err.to_string().contains("must be DateTime"));
    }

    #[test]
    fn parse_sliding_window_rejects_overflow_interval() {
        let sql = r#"
            CREATE TABLE t (block_number UInt64, ts DateTime, x Float64);
            CREATE MATERIALIZED VIEW mv AS
              SELECT SUM(x) AS total FROM t
              WINDOW SLIDING INTERVAL 999999999999999999 DAY BY ts;
        "#;
        let err = parse_schema(sql).unwrap_err();
        assert!(err.to_string().contains("overflows"));
    }
}
