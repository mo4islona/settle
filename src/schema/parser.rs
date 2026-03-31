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
                name,
                query,
                materialized: true,
                ..
            } => {
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
    let upper = input.to_uppercase();
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
    let upper = input.to_uppercase();
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

    let select = match query.body.as_ref() {
        sql::SetExpr::Select(s) => s,
        _ => {
            return Err(Error::Schema(format!(
                "MV '{view_name}': only simple SELECT supported"
            )));
        }
    };

    // Source table (FROM clause)
    let source = match select.from.first() {
        Some(sql::TableWithJoins { relation, .. }) => match relation {
            sql::TableFactor::Table { name, .. } => name.to_string(),
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
    for proj in &select.projection {
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
    let group_by = match &select.group_by {
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

    let seconds = match unit {
        sql::DateTimeField::Second => n,
        sql::DateTimeField::Minute => n * 60,
        sql::DateTimeField::Hour => n * 3600,
        sql::DateTimeField::Day => n * 86400,
        _ => {
            return Err(Error::Schema(format!(
                "MV '{view_name}': unsupported interval unit '{unit}'"
            )));
        }
    };

    Ok(seconds)
}

fn expr_to_column_name(expr: &sql::Expr) -> Result<String, Error> {
    match expr {
        sql::Expr::Identifier(ident) => Ok(ident.value.clone()),
        // sqlparser may parse some identifiers (like `user`) as functions with no args
        sql::Expr::Function(func) if matches!(&func.args, sql::FunctionArguments::None) => {
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
    let upper = input.to_uppercase();
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
    let upper_here = input[pos..].to_uppercase();
    if upper_here.starts_with("LANGUAGE") {
        pos = skip_keyword(input, pos, "LANGUAGE")?;
        pos = skip_ws(input, pos);
        pos = skip_keyword(input, pos, "LUA")?;
        pos = skip_ws(input, pos);
    }

    // Optional "AS"
    let upper_here = input[pos..].to_uppercase();
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
    let upper = input.to_uppercase();
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

    // GROUP BY
    pos = skip_keyword(input, pos, "GROUP")?;
    pos = skip_ws(input, pos);
    pos = skip_keyword(input, pos, "BY")?;
    pos = skip_ws(input, pos);
    let (group_by, new_pos) = read_identifier_list(input, pos)?;
    pos = skip_ws(input, new_pos);

    // STATE (...)
    pos = skip_keyword(input, pos, "STATE")?;
    pos = skip_ws(input, pos);
    let (state, new_pos) = parse_state_block(input, pos)?;
    pos = skip_ws(input, new_pos);

    // Optional REQUIRE clause (before body)
    let mut requires = Vec::new();
    let upper_here = input[pos..].to_uppercase();
    if upper_here.starts_with("REQUIRE") {
        pos = skip_keyword(input, pos, "REQUIRE")?;
        pos = skip_ws(input, pos);
        let (req_list, new_pos) = read_identifier_list(input, pos)?;
        requires = req_list;
        pos = skip_ws(input, new_pos);
    }

    // Body: either LANGUAGE lua PROCESS $$...$$ or LANGUAGE EXTERNAL or WHEN blocks
    let (mut body, new_pos) = parse_reducer_body(input, pos)?;
    pos = new_pos;

    // Fill in External id with reducer name if empty
    if let ReducerBody::External { ref mut id } = body {
        if id.is_empty() {
            *id = name.clone();
        }
    }

    // Skip optional END keyword and trailing semicolon
    pos = skip_ws(input, pos);
    let remaining_upper = input[pos..].to_uppercase();
    if remaining_upper.starts_with("END") {
        pos += 3;
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
        let column_type = str_to_column_type(&type_name)?;

        pos = skip_keyword(input, pos, "DEFAULT")?;
        pos = skip_ws(input, pos);

        let (default, new_pos) = read_token(input, pos)?;
        pos = new_pos;

        fields.push(StateField {
            name: field_name,
            column_type,
            default,
        });
    }

    Ok((fields, pos))
}

fn parse_reducer_body(input: &str, start: usize) -> Result<(ReducerBody, usize), Error> {
    let upper = input[start..].to_uppercase();
    if upper.starts_with("LANGUAGE") {
        // Check for LANGUAGE EXTERNAL vs LANGUAGE LUA
        let after_lang = upper["LANGUAGE".len()..].trim_start();
        if after_lang.starts_with("EXTERNAL") {
            parse_external_body(input, start)
        } else {
            parse_lua_body(input, start)
        }
    } else if upper.starts_with("WHEN") || upper.starts_with("ALWAYS") {
        parse_event_rules_body(input, start)
    } else {
        Err(Error::Schema(format!(
            "expected WHEN, LANGUAGE, or LANGUAGE EXTERNAL after STATE, got: '{}'",
            &input[start..std::cmp::min(start + 30, input.len())]
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
        "Lua reducers require the 'lua' feature. Use LANGUAGE EXTERNAL or event rules instead."
            .into(),
    ));
    #[cfg(feature = "lua")]
    Ok((ReducerBody::Lua { script }, new_pos))
}

fn parse_event_rules_body(input: &str, start: usize) -> Result<(ReducerBody, usize), Error> {
    let mut pos = start;
    let mut when_blocks = Vec::new();
    let mut always_emit = None;

    loop {
        pos = skip_ws(input, pos);
        let upper = input[pos..].to_uppercase();

        if upper.starts_with("WHEN") {
            let (block, new_pos) = parse_when_block(input, pos)?;
            when_blocks.push(block);
            pos = new_pos;
        } else if upper.starts_with("ALWAYS") {
            let (ae, new_pos) = parse_always_emit(input, pos)?;
            always_emit = Some(ae);
            pos = new_pos;
            break;
        } else {
            break;
        }
    }

    if when_blocks.is_empty() {
        return Err(Error::Schema(
            "reducer must have at least one WHEN block".into(),
        ));
    }

    Ok((
        ReducerBody::EventRules {
            when_blocks,
            always_emit,
        },
        pos,
    ))
}

fn parse_when_block(input: &str, start: usize) -> Result<(WhenBlock, usize), Error> {
    let mut pos = start;
    pos = skip_keyword(input, pos, "WHEN")?;
    pos = skip_ws(input, pos);

    // Read condition up to THEN
    let (condition_str, new_pos) = read_until_keyword(input, pos, "THEN")?;
    let condition = parse_expr(&condition_str)?;
    pos = new_pos;
    pos = skip_keyword(input, pos, "THEN")?;
    pos = skip_ws(input, pos);

    let mut lets = Vec::new();
    let mut sets = Vec::new();
    let mut emits = Vec::new();

    loop {
        pos = skip_ws(input, pos);
        let upper = input[pos..].to_uppercase();

        if upper.starts_with("LET ") {
            let (l, new_pos) = parse_let_clause(input, pos)?;
            lets.push(l);
            pos = new_pos;
        } else if upper.starts_with("SET ") {
            let (s, new_pos) = parse_set_clause(input, pos)?;
            sets.extend(s);
            pos = new_pos;
        } else if upper.starts_with("EMIT ") {
            let (e, new_pos) = parse_emit_clause(input, pos)?;
            emits.extend(e);
            pos = new_pos;
        } else {
            break;
        }
    }

    Ok((
        WhenBlock {
            condition,
            lets,
            sets,
            emits,
        },
        pos,
    ))
}

fn parse_always_emit(input: &str, start: usize) -> Result<(AlwaysEmit, usize), Error> {
    let mut pos = start;
    pos = skip_keyword(input, pos, "ALWAYS")?;
    pos = skip_ws(input, pos);
    pos = skip_keyword(input, pos, "EMIT")?;
    pos = skip_ws(input, pos);

    let (emits, new_pos) = parse_emit_list(input, pos)?;
    Ok((AlwaysEmit { emits }, new_pos))
}

fn parse_let_clause(input: &str, start: usize) -> Result<((String, Expr), usize), Error> {
    let mut pos = start;
    pos = skip_keyword(input, pos, "LET")?;
    pos = skip_ws(input, pos);

    let (name, new_pos) = read_identifier(input, pos)?;
    pos = skip_ws(input, new_pos);

    if pos < input.len() && input.as_bytes()[pos] == b'=' {
        pos += 1;
    } else {
        return Err(Error::Schema(format!("expected '=' after LET {name}")));
    }
    pos = skip_ws(input, pos);

    let (expr_str, new_pos) = read_expr_until_clause_boundary(input, pos)?;
    let expr = parse_expr(&expr_str)?;

    Ok(((name, expr), new_pos))
}

fn parse_set_clause(input: &str, start: usize) -> Result<(Vec<(String, Expr)>, usize), Error> {
    let mut pos = start;
    pos = skip_keyword(input, pos, "SET")?;
    pos = skip_ws(input, pos);

    let mut assignments = Vec::new();

    loop {
        pos = skip_ws(input, pos);

        // Read "state.field = expr"
        let (field_ref, new_pos) = read_dotted_name(input, pos)?;
        pos = skip_ws(input, new_pos);

        if pos < input.len() && input.as_bytes()[pos] == b'=' {
            pos += 1;
        } else {
            return Err(Error::Schema(format!("expected '=' after SET {field_ref}")));
        }
        pos = skip_ws(input, pos);

        let (expr_str, new_pos) = read_expr_until_comma_or_clause(input, pos)?;
        let expr = parse_expr(&expr_str)?;
        pos = new_pos;

        // Strip "state." prefix
        let field_name = field_ref
            .strip_prefix("state.")
            .unwrap_or(&field_ref)
            .to_string();
        assignments.push((field_name, expr));

        pos = skip_ws(input, pos);
        if pos < input.len() && input.as_bytes()[pos] == b',' {
            pos += 1;
        } else {
            break;
        }
    }

    Ok((assignments, pos))
}

fn parse_emit_clause(input: &str, start: usize) -> Result<(Vec<(String, Expr)>, usize), Error> {
    let mut pos = start;
    pos = skip_keyword(input, pos, "EMIT")?;
    pos = skip_ws(input, pos);
    parse_emit_list(input, pos)
}

fn parse_emit_list(input: &str, start: usize) -> Result<(Vec<(String, Expr)>, usize), Error> {
    let mut pos = start;
    let mut emits = Vec::new();

    loop {
        pos = skip_ws(input, pos);

        let (expr_str, new_pos) = read_expr_until_comma_or_clause(input, pos)?;
        pos = new_pos;

        // Check for "expr AS alias" or "name = expr"
        let trimmed = expr_str.trim();
        if let Some(eq_pos) = trimmed.find('=') {
            // name = expr form
            let name = trimmed[..eq_pos].trim().to_string();
            let expr = parse_expr(trimmed[eq_pos + 1..].trim())?;
            emits.push((name, expr));
        } else if let Some((expr_part, alias)) = split_as_alias(trimmed) {
            let expr = parse_expr(expr_part)?;
            emits.push((alias.to_string(), expr));
        } else {
            let expr = parse_expr(trimmed)?;
            // Use expression text as name fallback
            let name = trimmed.replace("state.", "").replace("row.", "");
            emits.push((name, expr));
        }

        pos = skip_ws(input, pos);
        if pos < input.len() && input.as_bytes()[pos] == b',' {
            pos += 1;
        } else {
            break;
        }
    }

    Ok((emits, pos))
}

fn split_as_alias(s: &str) -> Option<(&str, &str)> {
    // Find " AS " (case-insensitive) that's not inside parens.
    // Uses char_indices() for correct byte offsets with non-ASCII input.
    let upper = s.to_uppercase();
    let mut depth = 0;
    for (i, c) in s.char_indices() {
        match c {
            '(' => depth += 1,
            ')' => depth -= 1,
            _ => {}
        }
        if depth == 0 && upper[i..].starts_with(" AS ") {
            return Some((s[..i].trim(), s[i + 4..].trim()));
        }
    }
    None
}

// --- Expression parser ---

pub fn parse_expr(input: &str) -> Result<Expr, Error> {
    let input = input.trim();
    if input.is_empty() {
        return Err(Error::Schema("empty expression".into()));
    }
    parse_or_expr(input)
}

fn parse_or_expr(input: &str) -> Result<Expr, Error> {
    let parts = split_binary_op(input, &[" OR ", " or "]);
    if parts.len() == 1 {
        return parse_and_expr(parts[0].trim());
    }
    let mut expr = parse_and_expr(parts[0].trim())?;
    for part in &parts[1..] {
        let right = parse_and_expr(part.trim())?;
        expr = Expr::BinaryOp {
            left: Box::new(expr),
            op: BinaryOp::Or,
            right: Box::new(right),
        };
    }
    Ok(expr)
}

fn parse_and_expr(input: &str) -> Result<Expr, Error> {
    let parts = split_binary_op(input, &[" AND ", " and "]);
    if parts.len() == 1 {
        return parse_comparison_expr(parts[0].trim());
    }
    let mut expr = parse_comparison_expr(parts[0].trim())?;
    for part in &parts[1..] {
        let right = parse_comparison_expr(part.trim())?;
        expr = Expr::BinaryOp {
            left: Box::new(expr),
            op: BinaryOp::And,
            right: Box::new(right),
        };
    }
    Ok(expr)
}

fn parse_comparison_expr(input: &str) -> Result<Expr, Error> {
    // Check for IF(cond, then, else) at top level
    let upper = input.to_uppercase();
    if upper.starts_with("IF(") || upper.starts_with("IF (") {
        return parse_if_expr(input);
    }

    // Try comparison operators: >=, <=, !=, =, >, <
    for (op_str, op) in &[
        (">=", BinaryOp::Gte),
        ("<=", BinaryOp::Lte),
        ("!=", BinaryOp::Neq),
        ("=", BinaryOp::Eq),
        (">", BinaryOp::Gt),
        ("<", BinaryOp::Lt),
    ] {
        if let Some((left, right)) = split_at_op_outside_parens(input, op_str) {
            return Ok(Expr::BinaryOp {
                left: Box::new(parse_additive_expr(left.trim())?),
                op: op.clone(),
                right: Box::new(parse_additive_expr(right.trim())?),
            });
        }
    }

    parse_additive_expr(input)
}

fn parse_if_expr(input: &str) -> Result<Expr, Error> {
    // IF(cond, then, else)
    let inner_start = input
        .find('(')
        .ok_or_else(|| Error::Schema("IF missing '('".into()))?
        + 1;
    let inner_end = rfind_matching_paren(input, inner_start - 1)?;
    let inner = &input[inner_start..inner_end];

    // Split on commas at depth 0
    let parts = split_top_level_commas(inner);
    if parts.len() != 3 {
        return Err(Error::Schema(format!(
            "IF() requires 3 arguments, got {}: '{inner}'",
            parts.len()
        )));
    }

    Ok(Expr::If {
        condition: Box::new(parse_expr(parts[0])?),
        then_expr: Box::new(parse_expr(parts[1])?),
        else_expr: Box::new(parse_expr(parts[2])?),
    })
}

fn parse_additive_expr(input: &str) -> Result<Expr, Error> {
    let input = input.trim();

    // Split on + or - at top level (not inside parens), right-to-left for left-assoc
    let mut depth = 0i32;
    let bytes = input.as_bytes();
    let mut last_op_pos = None;

    for i in 0..bytes.len() {
        match bytes[i] {
            b'(' => depth += 1,
            b')' => depth -= 1,
            b'+' | b'-' if depth == 0 && i > 0 => {
                // Make sure it's not part of a comparison operator or scientific notation
                if i + 1 < bytes.len() && (bytes[i + 1] == b'=' || bytes[i + 1] == b'>') {
                    continue;
                }
                // Check it's not a unary minus (preceded by operator or start)
                let prev = bytes[i - 1];
                if prev == b'(' || prev == b'=' || prev == b',' || prev == b'*' || prev == b'/' {
                    continue;
                }
                last_op_pos = Some(i);
            }
            _ => {}
        }
    }

    if let Some(pos) = last_op_pos {
        let left = &input[..pos];
        let right = &input[pos + 1..];
        let op = if bytes[pos] == b'+' {
            BinaryOp::Add
        } else {
            BinaryOp::Sub
        };
        return Ok(Expr::BinaryOp {
            left: Box::new(parse_additive_expr(left.trim())?),
            op,
            right: Box::new(parse_multiplicative_expr(right.trim())?),
        });
    }

    parse_multiplicative_expr(input)
}

fn parse_multiplicative_expr(input: &str) -> Result<Expr, Error> {
    let input = input.trim();

    let mut depth = 0i32;
    let bytes = input.as_bytes();
    let mut last_op_pos = None;

    for i in 0..bytes.len() {
        match bytes[i] {
            b'(' => depth += 1,
            b')' => depth -= 1,
            b'*' | b'/' if depth == 0 => {
                last_op_pos = Some(i);
            }
            _ => {}
        }
    }

    if let Some(pos) = last_op_pos {
        let left = &input[..pos];
        let right = &input[pos + 1..];
        let op = if bytes[pos] == b'*' {
            BinaryOp::Mul
        } else {
            BinaryOp::Div
        };
        return Ok(Expr::BinaryOp {
            left: Box::new(parse_multiplicative_expr(left.trim())?),
            op,
            right: Box::new(parse_atom_expr(right.trim())?),
        });
    }

    parse_atom_expr(input)
}

fn parse_atom_expr(input: &str) -> Result<Expr, Error> {
    let input = input.trim();

    if input.is_empty() {
        return Err(Error::Schema("unexpected empty expression".into()));
    }

    // Parenthesized expression
    if input.starts_with('(') && input.ends_with(')') {
        let inner = &input[1..input.len() - 1];
        // Verify parens are balanced
        if is_balanced_parens(inner) {
            return parse_expr(inner);
        }
    }

    // IF function
    let upper = input.to_uppercase();
    if upper.starts_with("IF(") || upper.starts_with("IF (") {
        return parse_if_expr(input);
    }

    // Numeric literals
    if let Ok(v) = input.parse::<i64>() {
        return Ok(Expr::Int(v));
    }
    if let Ok(v) = input.parse::<f64>() {
        return Ok(Expr::Float(v));
    }

    // String literals
    if (input.starts_with('\'') && input.ends_with('\''))
        || (input.starts_with('"') && input.ends_with('"'))
    {
        return Ok(Expr::Literal(input[1..input.len() - 1].to_string()));
    }

    // state.xxx or row.xxx or plain column ref
    if let Some(field) = input.strip_prefix("state.") {
        return Ok(Expr::StateRef(field.to_string()));
    }
    if let Some(field) = input.strip_prefix("row.") {
        return Ok(Expr::RowRef(field.to_string()));
    }

    // Plain identifier
    if input.chars().all(|c| c.is_alphanumeric() || c == '_') {
        return Ok(Expr::ColumnRef(input.to_string()));
    }

    Err(Error::Schema(format!("cannot parse expression: '{input}'")))
}

// --- Utility functions ---

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
    let upper = remaining.to_uppercase();
    if upper.starts_with(keyword) {
        let after = pos + keyword.len();
        if after >= input.len() || !input.as_bytes()[after].is_ascii_alphanumeric() {
            return Ok(after);
        }
    }
    Err(Error::Schema(format!(
        "expected '{keyword}' at position {pos}, got: '{}'",
        &input[pos..std::cmp::min(pos + 20, input.len())]
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
            &input[pos..std::cmp::min(pos + 10, input.len())]
        )));
    }
    Ok((input[start..p].to_string(), p))
}

fn read_dotted_name(input: &str, pos: usize) -> Result<(String, usize), Error> {
    let bytes = input.as_bytes();
    let start = pos;
    let mut p = pos;
    while p < bytes.len()
        && (bytes[p].is_ascii_alphanumeric() || bytes[p] == b'_' || bytes[p] == b'.')
    {
        p += 1;
    }
    if p == start {
        return Err(Error::Schema(format!("expected name at position {pos}")));
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

fn read_until_keyword(input: &str, pos: usize, keyword: &str) -> Result<(String, usize), Error> {
    let remaining = &input[pos..];
    let upper = remaining.to_uppercase();
    let kw_upper = keyword.to_uppercase();

    // Search for the keyword preceded by whitespace and followed by whitespace
    let mut search_from = 0;
    while search_from < upper.len() {
        if let Some(idx) = upper[search_from..].find(&kw_upper) {
            let abs_idx = search_from + idx;
            let before_ok = abs_idx == 0 || remaining.as_bytes()[abs_idx - 1].is_ascii_whitespace();
            let after_pos = abs_idx + kw_upper.len();
            let after_ok = after_pos >= remaining.len()
                || remaining.as_bytes()[after_pos].is_ascii_whitespace();

            if before_ok && after_ok {
                return Ok((input[pos..pos + abs_idx].trim().to_string(), pos + abs_idx));
            }
            search_from = abs_idx + 1;
        } else {
            break;
        }
    }

    Err(Error::Schema(format!("expected '{keyword}' not found")))
}

fn read_expr_until_clause_boundary(input: &str, pos: usize) -> Result<(String, usize), Error> {
    read_expr_until_any(input, pos, &["WHEN", "ALWAYS", "SET", "EMIT", "LET"])
}

fn read_expr_until_comma_or_clause(input: &str, pos: usize) -> Result<(String, usize), Error> {
    let bytes = input.as_bytes();
    let mut p = pos;
    let mut depth = 0i32;

    while p < bytes.len() {
        match bytes[p] {
            b'(' => {
                depth += 1;
                p += 1;
            }
            b')' => {
                if depth == 0 {
                    break;
                }
                depth -= 1;
                p += 1;
            }
            b',' if depth == 0 => break,
            b';' if depth == 0 => break,
            _ if depth == 0 => {
                // Check for clause keywords
                let remaining_upper = input[p..].to_uppercase();
                for kw in &["WHEN ", "ALWAYS ", "SET ", "EMIT ", "LET ", "END;", "END "] {
                    if remaining_upper.starts_with(kw) {
                        let trimmed = input[pos..p].trim();
                        if !trimmed.is_empty() {
                            return Ok((trimmed.to_string(), p));
                        }
                    }
                }
                p += 1;
            }
            _ => {
                p += 1;
            }
        }
    }

    let result = input[pos..p].trim().to_string();
    Ok((result, p))
}

fn read_expr_until_any(
    input: &str,
    pos: usize,
    keywords: &[&str],
) -> Result<(String, usize), Error> {
    let bytes = input.as_bytes();
    let mut p = pos;
    let mut depth = 0i32;

    while p < bytes.len() {
        match bytes[p] {
            b'(' => depth += 1,
            b')' => {
                if depth == 0 {
                    break;
                }
                depth -= 1;
            }
            b';' if depth == 0 => break,
            _ if depth == 0 => {
                let remaining_upper = input[p..].to_uppercase();
                for kw in keywords {
                    let kw_space = format!("{kw} ");
                    if remaining_upper.starts_with(&kw_space)
                        || remaining_upper.starts_with(&format!("{kw}\n"))
                    {
                        return Ok((input[pos..p].trim().to_string(), p));
                    }
                }
            }
            _ => {}
        }
        p += 1;
    }

    Ok((input[pos..p].trim().to_string(), p))
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

fn split_binary_op<'a>(input: &'a str, ops: &[&str]) -> Vec<&'a str> {
    let mut parts = Vec::new();
    let mut last = 0;
    let mut depth = 0i32;
    let bytes = input.as_bytes();
    let mut i = 0;

    while i < bytes.len() {
        match bytes[i] {
            b'(' => {
                depth += 1;
                i += 1;
            }
            b')' => {
                depth -= 1;
                i += 1;
            }
            // Skip quoted strings to avoid matching operators inside them
            q @ (b'\'' | b'"') => {
                i += 1;
                while i < bytes.len() {
                    if bytes[i] == q {
                        i += 1;
                        if i < bytes.len() && bytes[i] == q {
                            i += 1; // doubled-quote escape
                        } else {
                            break;
                        }
                    } else {
                        i += 1;
                    }
                }
            }
            // Operators are pure ASCII — match case-insensitively on raw bytes
            // to avoid to_uppercase() byte-length misalignment with non-ASCII input.
            _ if depth == 0 => {
                let mut matched = false;
                for op in ops {
                    let ob = op.as_bytes();
                    if i + ob.len() <= bytes.len()
                        && bytes[i..i + ob.len()].eq_ignore_ascii_case(ob)
                    {
                        parts.push(&input[last..i]);
                        last = i + ob.len();
                        i = last;
                        matched = true;
                        break;
                    }
                }
                if !matched {
                    i += 1;
                }
            }
            _ => {
                i += 1;
            }
        }
    }
    parts.push(&input[last..]);
    parts
}

fn split_at_op_outside_parens<'a>(input: &'a str, op: &str) -> Option<(&'a str, &'a str)> {
    let mut depth = 0i32;
    let bytes = input.as_bytes();
    let op_bytes = op.as_bytes();

    for i in 0..bytes.len() {
        match bytes[i] {
            b'(' => depth += 1,
            b')' => depth -= 1,
            _ if depth == 0 && i + op_bytes.len() <= bytes.len() => {
                if &bytes[i..i + op_bytes.len()] == op_bytes {
                    // For '=', make sure it's not part of >=, <=, !=
                    if op == "=" && i > 0 {
                        let prev = bytes[i - 1];
                        if prev == b'>' || prev == b'<' || prev == b'!' {
                            continue;
                        }
                    }
                    return Some((&input[..i], &input[i + op.len()..]));
                }
            }
            _ => {}
        }
    }
    None
}

fn split_top_level_commas(input: &str) -> Vec<&str> {
    let mut parts = Vec::new();
    let mut depth = 0i32;
    let mut last = 0;

    for (i, b) in input.bytes().enumerate() {
        match b {
            b'(' => depth += 1,
            b')' => depth -= 1,
            b',' if depth == 0 => {
                parts.push(input[last..i].trim());
                last = i + 1;
            }
            _ => {}
        }
    }
    parts.push(input[last..].trim());
    parts
}

fn rfind_matching_paren(input: &str, open_pos: usize) -> Result<usize, Error> {
    let bytes = input.as_bytes();
    let mut depth = 0i32;
    for i in open_pos..bytes.len() {
        match bytes[i] {
            b'(' => depth += 1,
            b')' => {
                depth -= 1;
                if depth == 0 {
                    return Ok(i);
                }
            }
            _ => {}
        }
    }
    Err(Error::Schema("unmatched parenthesis".into()))
}

fn is_balanced_parens(input: &str) -> bool {
    let mut depth = 0i32;
    for b in input.bytes() {
        match b {
            b'(' => depth += 1,
            b')' => {
                depth -= 1;
                if depth < 0 {
                    return false;
                }
            }
            _ => {}
        }
    }
    depth == 0
}

// --- Schema validation ---

fn validate_schema(schema: &Schema) -> Result<(), Error> {
    let table_names: Vec<&str> = schema.tables.iter().map(|t| t.name.as_str()).collect();
    let reducer_names: Vec<&str> = schema.reducers.iter().map(|r| r.name.as_str()).collect();
    let module_names: Vec<&str> = schema.modules.iter().map(|m| m.name.as_str()).collect();

    // Validate module name uniqueness
    {
        let mut seen = std::collections::HashSet::new();
        for name in &module_names {
            if !seen.insert(name) {
                return Err(Error::Schema(format!("duplicate module name: '{name}'")));
            }
        }
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

    #[test]
    fn parse_event_rules_reducer() {
        let sql = r#"
            CREATE TABLE trades (
                block_number UInt64,
                block_time   DateTime,
                user         String,
                token        String,
                side         String,
                amount       Float64,
                price        Float64
            );

            CREATE REDUCER pnl_tracker
              SOURCE trades
              GROUP BY user, token
              STATE (
                quantity   Float64 DEFAULT 0,
                cost_basis Float64 DEFAULT 0
              )

              WHEN row.side = 'buy' THEN
                SET state.quantity   = state.quantity + row.amount,
                    state.cost_basis = state.cost_basis + row.amount * row.price
                EMIT trade_pnl = 0

              WHEN row.side = 'sell' THEN
                LET avg_cost = state.cost_basis / state.quantity
                SET state.quantity   = state.quantity - row.amount,
                    state.cost_basis = state.cost_basis - row.amount * avg_cost
                EMIT trade_pnl = row.amount * (row.price - avg_cost)

              ALWAYS EMIT
                state.quantity AS position_size,
                IF(state.quantity > 0, state.cost_basis / state.quantity, 0) AS avg_cost;
        "#;
        let schema = parse_schema(sql).unwrap();
        assert_eq!(schema.tables.len(), 1);
        assert_eq!(schema.reducers.len(), 1);

        let r = &schema.reducers[0];
        assert_eq!(r.name, "pnl_tracker");
        assert_eq!(r.source, "trades");
        assert_eq!(r.group_by, vec!["user", "token"]);
        assert_eq!(r.state.len(), 2);
        assert_eq!(r.state[0].name, "quantity");
        assert_eq!(r.state[0].column_type, ColumnType::Float64);
        assert_eq!(r.state[0].default, "0");
        assert_eq!(r.state[1].name, "cost_basis");

        match &r.body {
            ReducerBody::EventRules {
                when_blocks,
                always_emit,
            } => {
                assert_eq!(when_blocks.len(), 2);

                // Buy block
                assert_eq!(when_blocks[0].sets.len(), 2);
                assert_eq!(when_blocks[0].emits.len(), 1);
                assert_eq!(when_blocks[0].lets.len(), 0);

                // Sell block
                assert_eq!(when_blocks[1].lets.len(), 1);
                assert_eq!(when_blocks[1].lets[0].0, "avg_cost");
                assert_eq!(when_blocks[1].sets.len(), 2);
                assert_eq!(when_blocks[1].emits.len(), 1);

                // Always emit
                let ae = always_emit.as_ref().unwrap();
                assert_eq!(ae.emits.len(), 2);
                assert_eq!(ae.emits[0].0, "position_size");
                assert_eq!(ae.emits[1].0, "avg_cost");
            }
            _ => panic!("expected EventRules"),
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

    #[test]
    fn validation_rejects_bad_reducer_source() {
        let sql = r#"
            CREATE REDUCER bad_reducer
              SOURCE nonexistent_table
              GROUP BY user
              STATE (qty Float64 DEFAULT 0)

              WHEN row.side = 'buy' THEN
                SET state.qty = state.qty + 1
                EMIT count = state.qty;
        "#;
        let err = parse_schema(sql).unwrap_err();
        assert!(err.to_string().contains("nonexistent_table"));
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

              WHEN row.price > 0 THEN
                SET state.qty = state.qty + 1
                EMIT count = state.qty;
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

              WHEN row.amount_in > 0 THEN
                LET avg_cost = state.cost_basis / state.quantity
                SET state.quantity   = state.quantity - row.amount_in,
                    state.cost_basis = state.cost_basis - row.amount_in * avg_cost
                EMIT trade_pnl = row.amount_in * (row.price - avg_cost)

              WHEN row.amount_out > 0 THEN
                SET state.quantity   = state.quantity + row.amount_out,
                    state.cost_basis = state.cost_basis + row.amount_out * row.price
                EMIT trade_pnl = 0

              ALWAYS EMIT
                state.quantity AS position_size,
                IF(state.quantity > 0, state.cost_basis / state.quantity, 0) AS avg_cost;

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

    #[test]
    fn parse_expr_arithmetic() {
        let expr = parse_expr("state.quantity + row.amount").unwrap();
        match expr {
            Expr::BinaryOp {
                left,
                op: BinaryOp::Add,
                right,
            } => {
                assert!(matches!(*left, Expr::StateRef(ref s) if s == "quantity"));
                assert!(matches!(*right, Expr::RowRef(ref s) if s == "amount"));
            }
            _ => panic!("expected BinaryOp::Add, got {expr:?}"),
        }
    }

    #[test]
    fn parse_expr_multiply_and_subtract() {
        let expr = parse_expr("row.amount * (row.price - avg_cost)").unwrap();
        match expr {
            Expr::BinaryOp {
                op: BinaryOp::Mul, ..
            } => {}
            _ => panic!("expected Mul at top level, got {expr:?}"),
        }
    }

    #[test]
    fn parse_expr_if_function() {
        let expr =
            parse_expr("IF(state.quantity > 0, state.cost_basis / state.quantity, 0)").unwrap();
        match expr {
            Expr::If {
                condition,
                then_expr,
                else_expr,
            } => {
                assert!(matches!(
                    *condition,
                    Expr::BinaryOp {
                        op: BinaryOp::Gt,
                        ..
                    }
                ));
                assert!(matches!(
                    *then_expr,
                    Expr::BinaryOp {
                        op: BinaryOp::Div,
                        ..
                    }
                ));
                assert!(matches!(*else_expr, Expr::Int(0)));
            }
            _ => panic!("expected If, got {expr:?}"),
        }
    }

    #[test]
    fn parse_expr_string_comparison() {
        let expr = parse_expr("row.side = 'buy'").unwrap();
        match expr {
            Expr::BinaryOp {
                left,
                op: BinaryOp::Eq,
                right,
            } => {
                assert!(matches!(*left, Expr::RowRef(ref s) if s == "side"));
                assert!(matches!(*right, Expr::Literal(ref s) if s == "buy"));
            }
            _ => panic!("expected Eq, got {expr:?}"),
        }
    }

    /// Issue #8: OR conditions must be parsed correctly.
    #[test]
    fn parse_expr_or_condition() {
        let expr = parse_expr("row.side = 'buy' OR row.side = 'sell'").unwrap();
        match expr {
            Expr::BinaryOp {
                op: BinaryOp::Or,
                left,
                right,
            } => {
                match *left {
                    Expr::BinaryOp {
                        op: BinaryOp::Eq, ..
                    } => {}
                    other => panic!("expected Eq on left, got {other:?}"),
                }
                match *right {
                    Expr::BinaryOp {
                        op: BinaryOp::Eq, ..
                    } => {}
                    other => panic!("expected Eq on right, got {other:?}"),
                }
            }
            other => panic!("expected Or, got {other:?}"),
        }
    }

    /// Issue #8: OR has lower precedence than AND.
    #[test]
    fn parse_expr_or_and_precedence() {
        // "a = 1 OR b = 2 AND c = 3" should parse as "a = 1 OR (b = 2 AND c = 3)"
        let expr = parse_expr("row.a = '1' OR row.b = '2' AND row.c = '3'").unwrap();
        match expr {
            Expr::BinaryOp {
                op: BinaryOp::Or,
                right,
                ..
            } => match *right {
                Expr::BinaryOp {
                    op: BinaryOp::And, ..
                } => {}
                other => panic!("expected And on right side of Or, got {other:?}"),
            },
            other => panic!("expected Or at top level, got {other:?}"),
        }
    }

    /// split_binary_op must not panic on multibyte UTF-8 in quoted strings.
    #[test]
    fn parse_expr_multibyte_utf8_in_condition() {
        let expr = parse_expr("row.msg = 'café' OR row.msg = 'naïve'").unwrap();
        match expr {
            Expr::BinaryOp {
                op: BinaryOp::Or, ..
            } => {}
            other => panic!("expected Or, got {other:?}"),
        }
    }

    /// to_uppercase() changes byte length for some chars (e.g. 'ß' → "SS").
    /// split_binary_op must handle this without index misalignment.
    #[test]
    fn parse_expr_uppercase_length_change() {
        // 'ß' is 2 bytes, uppercases to "SS" (2 bytes → 2 chars but same byte count here).
        // Use a more extreme case: column named with non-ASCII that changes length.
        let expr = parse_expr("row.x = 'straße' AND row.y = '1'").unwrap();
        match expr {
            Expr::BinaryOp {
                op: BinaryOp::And, ..
            } => {}
            other => panic!("expected And, got {other:?}"),
        }
    }

    /// Issue #24: read_token must handle SQL doubled-quote escaping.
    #[test]
    fn read_token_handles_doubled_quotes() {
        let (token, end) = read_token("'it''s fine' rest", 0).unwrap();
        assert_eq!(token, "'it''s fine'");
        assert_eq!(end, 12);
    }

    /// Issue #23: split_as_alias must handle non-ASCII characters before AS keyword.
    #[test]
    fn parse_mv_with_non_ascii_alias_source() {
        // Non-ASCII chars before AS keyword — would panic with char-count indexing
        let result = split_as_alias("SUM(café) AS total");
        assert!(result.is_some());
        let (expr, alias) = result.unwrap();
        assert_eq!(expr, "SUM(café)");
        assert_eq!(alias, "total");
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

              WHEN row.amount > 0 THEN
                SET state.total_received = state.total_received + row.amount
                EMIT balance = state.total_received;
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
                  SELECT SUM(x) AS total FROM t GROUP BY x
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
              SELECT SUM(x) AS total FROM t GROUP BY x;
        "#;
        let schema = parse_schema(sql).unwrap();
        assert!(schema.materialized_views[0].sliding_window.is_none());
    }

    #[test]
    fn parse_sliding_window_rejects_missing_interval() {
        let sql = r#"
            CREATE TABLE t (block_number UInt64, ts DateTime, x Float64);
            CREATE MATERIALIZED VIEW mv AS
              SELECT SUM(x) AS total FROM t GROUP BY x
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
              SELECT SUM(x) AS total FROM t GROUP BY x
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
              SELECT SUM(x) AS total FROM t GROUP BY x
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
              SELECT SUM(x) AS total FROM t GROUP BY x
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
              SELECT SUM(x) AS total FROM t GROUP BY x
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
              SELECT SUM(x) AS total FROM t GROUP BY x
              WINDOW SLIDING INTERVAL 999999999999999999 DAY BY ts;
        "#;
        let err = parse_schema(sql).unwrap_err();
        assert!(err.to_string().contains("overflows"));
    }
}
