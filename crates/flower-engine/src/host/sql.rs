use super::*;

struct SqlInput {
    rows: BTreeMap<Vec<u8>, Vec<u8>>,
    bytes: usize,
}

fn scan_failure() -> OpError {
    fail(
        flower_sql::SQL_BUDGET,
        "SQL input candidate or byte budget exceeded",
    )
    .into()
}

impl Host<'_> {
    pub(super) fn op_sql(&mut self, args: Value) -> OpResult<Value> {
        let args = expect_array(args, 3, "SQL takes [plan, params, explain]")?;
        let name = expect_str(&args[0], "SQL plan")?;
        let plan = self
            .descriptors
            .get(&self.session.module)
            .and_then(|module| module.sql.get(name))
            .cloned()
            .ok_or_else(|| {
                OpError::from(fail(
                    flower_sql::SQL_INVALID,
                    "SQL plan is not declared by the calling module",
                ))
            })?;
        let explain = match args[2] {
            Value::Bool(explain) => explain,
            _ => {
                return Err(OpError::from(fail(
                    flower_sql::SQL_INVALID,
                    "SQL explain must be boolean",
                )));
            }
        };
        if explain {
            return Ok(plan.explain());
        }
        self.execute_sql(&plan, &args[1])
    }

    pub(super) fn op_sql_dynamic(&mut self, args: Value) -> OpResult<Value> {
        let args = expect_array(
            args,
            4,
            "dynamic SQL takes [endpoint, statement, params, explain]",
        )?;
        let endpoint = expect_str(&args[0], "SQL endpoint")?;
        let endpoint = self
            .descriptors
            .get(&self.session.module)
            .and_then(|module| module.sql_endpoints.get(endpoint))
            .cloned()
            .ok_or_else(|| {
                OpError::from(fail(
                    flower_sql::SQL_INVALID,
                    "SQL endpoint is not declared by the calling module",
                ))
            })?;
        let statement = expect_str(&args[1], "SQL statement")?;
        let explain = match args[3] {
            Value::Bool(explain) => explain,
            _ => return Err(fail(flower_sql::SQL_INVALID, "SQL explain must be boolean").into()),
        };
        let work = flower_sql::binding_work(statement, &args[2]);
        self.session.spend(work.min(endpoint.budget.work.into()))?;
        if work >= u64::from(endpoint.budget.work)
            || statement.len() >= endpoint.budget.bytes as usize
        {
            return Err(fail(
                flower_sql::SQL_BUDGET,
                "SQL binding exceeds endpoint ceilings",
            )
            .into());
        }
        let collection = flower_sql::collection(statement)?;
        if !endpoint.collections.contains(&collection) {
            return Err(fail(
                codes::ACCESS_DENIED,
                "SQL source is outside this endpoint's collection scope",
            )
            .into());
        }
        let source = self.collection_def(&self.session.module, &collection)?;
        let budget = flower_sdk::SqlBudget::scan(
            endpoint.budget.candidates,
            endpoint.budget.bytes - statement.len() as u32,
            endpoint.budget.work - work as u32,
        );
        let indexes = source
            .indexes
            .iter()
            .map(|index| flower_sql::Index {
                name: index.name.clone(),
                fields: index.fields.clone(),
            })
            .collect::<Vec<_>>();
        let plan = flower_sql::Plan::bind_dynamic(
            statement,
            source.key_schema,
            source.value_schema,
            &args[2],
            budget,
            source.access.is_some(),
        )?
        .with_indexes(&indexes, endpoint.allow_scan)?;
        if explain {
            let Value::Object(mut info) = plan.explain() else {
                unreachable!("EXPLAIN is an object")
            };
            info.insert("binding_work".into(), Value::Number(work as f64));
            info.insert(
                "endpoint_max_work".into(),
                Value::Number(endpoint.budget.work.into()),
            );
            info.insert(
                "endpoint_max_bytes".into(),
                Value::Number(endpoint.budget.bytes.into()),
            );
            Ok(Value::Object(info))
        } else {
            self.execute_sql(&plan, &args[2])
        }
    }

    fn execute_sql(&mut self, plan: &flower_sql::Plan, params: &Value) -> OpResult<Value> {
        let access = plan.access(params)?;
        let module = self.session.module.clone();
        let name = plan.collection();
        self.check_read(&module, name, false)?;
        let collection = self.collection_id(&module, name)?;
        let budget = plan.budget();
        let bytes = plan.input_bytes_limit(params)?.min(budget.work as usize);
        let input = match access {
            flower_sql::Access::CollectionScan => {
                self.sql_scan(collection, budget.candidates as usize, bytes)
            }
            flower_sql::Access::PrimaryKey(key) => self.sql_key(collection, key, bytes),
            flower_sql::Access::IndexEquality { index, components } => {
                let (index, arity) = self.index_id(&module, name, index)?;
                if arity != components.len() {
                    return Err(fail(flower_sql::SQL_INVALID, "SQL index arity changed").into());
                }
                self.sql_index(
                    collection,
                    index,
                    &components,
                    budget.candidates as usize,
                    bytes,
                )
            }
            flower_sql::Access::Empty => Ok(SqlInput {
                rows: BTreeMap::new(),
                bytes: 0,
            }),
        };
        let input = match input {
            Ok(input) => input,
            Err(error) => {
                // A failed scan may already have decoded a prefix of its input.
                // Charge its work ceiling so catching the failure cannot evade
                // the invocation's shared fuel through repeated partial scans.
                if error.failure.code == flower_sql::SQL_BUDGET {
                    self.session.spend(budget.work.into())?;
                }
                return Err(error);
            }
        };
        let mut evaluator = plan.evaluator(params, input.bytes)?;
        let guard = self.guard(&module, name);
        let outcome = (|| {
            for (key, row) in input.rows {
                let key = parse_value(&key)?;
                let row = parse_value(&row)?;
                let row = match guard.as_deref() {
                    Some(policy) => {
                        self.session.spend(FUEL_ROW)?;
                        match self.shown(&module, policy, &key, row, &[])? {
                            Some(row) => row,
                            None => continue,
                        }
                    }
                    None => row,
                };
                evaluator.limit_work(self.session.fuel_left());
                evaluator.push(key, row)?;
            }
            evaluator.limit_work(self.session.fuel_left());
            evaluator.finish().map_err(OpError::from)
        })();
        // SQL's work ceiling is in addition to the invocation's shared fuel.
        self.session.spend(evaluator.work().into())?;
        Ok(Value::Array(outcome?))
    }

    fn sql_record(&self, collection: u32, key: &[u8], bytes: usize) -> OpResult<Option<Vec<u8>>> {
        if let Some(value) = self.session.staged.staged_record_view(collection, key) {
            if value.as_ref().is_some_and(|v| v.len() > bytes) {
                return Err(scan_failure());
            }
            return Ok(value.clone());
        }
        self.read
            .get_record_bounded(&self.session.db, collection, key, bytes)?
            .ok_or_else(scan_failure)
    }

    fn sql_key(&mut self, collection: u32, key: &Value, bytes: usize) -> OpResult<SqlInput> {
        self.session.spend(1)?;
        let bytes = bytes.min(self.session.fuel_left().min(usize::MAX as u64) as usize);
        let key = render(key)?;
        self.session
            .record_dep(encode_record_dep(collection, &key)?);
        let base = key.len().saturating_add(128);
        let available = bytes.checked_sub(base).ok_or_else(scan_failure)?;
        let value = self.sql_record(collection, &key, available)?;
        let mut rows = BTreeMap::new();
        let size = base + value.as_ref().map_or(0, Vec::len);
        if let Some(value) = value {
            rows.insert(key, value);
        }
        Ok(SqlInput { rows, bytes: size })
    }

    fn sql_scan(&mut self, collection: u32, candidates: usize, bytes: usize) -> OpResult<SqlInput> {
        // Errors can be cached too: observe the scope before scanning it.
        self.session.record_dep(encode_collection_range_dep(
            collection,
            &RangeDep::default(),
        )?);
        self.session.spend(candidates as u64)?;
        let bytes = bytes.min(self.session.fuel_left().min(usize::MAX as u64) as usize);
        let staged = self
            .session
            .staged
            .bounded_records_for(collection, candidates, bytes)
            .ok_or_else(scan_failure)?;
        let staged_bytes = staged
            .iter()
            .map(|(key, row)| key.len() + row.as_ref().map_or(0, Vec::len) + 128)
            .sum::<usize>();
        let committed = self
            .read
            .scan_records_bounded(
                &self.session.db,
                collection,
                candidates - staged.len(),
                bytes - staged_bytes,
            )?
            .ok_or_else(scan_failure)?;
        let used = staged_bytes
            + committed
                .iter()
                .map(|(key, row)| key.len() + row.len() + 128)
                .sum::<usize>();
        let mut rows: BTreeMap<_, _> = committed.into_iter().collect();
        for (key, value) in staged {
            match value {
                Some(value) => {
                    rows.insert(key, value);
                }
                None => {
                    rows.remove(&key);
                }
            }
        }
        Ok(SqlInput { rows, bytes: used })
    }

    fn sql_index(
        &mut self,
        collection: u32,
        index: u32,
        values: &[&Value],
        candidates: usize,
        bytes: usize,
    ) -> OpResult<SqlInput> {
        // One seek plus at most one row probe per candidate; reserve even on
        // exhaustion. A nonunique equality bucket has no cardinality proof.
        self.session.spend((2 * candidates + 1) as u64)?;
        let bytes = bytes.min(self.session.fuel_left().min(usize::MAX as u64) as usize);
        let mut components = Vec::new();
        let mut used = 0usize;
        for value in values {
            let part = render(value)?;
            used = used.saturating_add(part.len()).saturating_add(4);
            if used > bytes {
                return Err(scan_failure());
            }
            components.push(part);
        }
        self.session.record_dep(encode_range_dep(
            index,
            &RangeDep {
                prefix: components.clone(),
                ..Default::default()
            },
        )?);
        let staged = self
            .session
            .staged
            .bounded_index_keys(index, &components, candidates, bytes - used)
            .ok_or_else(scan_failure)?;
        used += staged.iter().map(|(key, _)| key.len() + 128).sum::<usize>();
        let refs: Vec<_> = components.iter().map(Vec::as_slice).collect();
        let committed = self
            .read
            .index_keys_bounded(
                &self.session.db,
                index,
                &refs,
                candidates - staged.len(),
                bytes - used,
            )?
            .ok_or_else(scan_failure)?;
        used += committed.iter().map(|key| key.len() + 128).sum::<usize>();
        let mut keys: BTreeMap<_, _> = committed.into_iter().map(|key| (key, true)).collect();
        keys.extend(staged);
        let mut rows = BTreeMap::new();
        for (key, present) in keys {
            if !present {
                continue;
            }
            // Non-indexed field changes do not necessarily touch an index entry.
            self.session
                .record_dep(encode_record_dep(collection, &key)?);
            let available = bytes
                .checked_sub(used.saturating_add(128))
                .ok_or_else(scan_failure)?;
            if let Some(row) = self.sql_record(collection, &key, available)? {
                used += row.len() + 128;
                rows.insert(key, row);
            }
        }
        Ok(SqlInput { rows, bytes: used })
    }
}
