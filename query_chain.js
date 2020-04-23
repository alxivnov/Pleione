const fs = require('fs');



module.exports = class QueryChain {
	constructor(db = null, log = null) {
		this._db = db;

		if (log) {
			if (log.includes('err'))
				this._logErr = true;
			if (log.includes('doc'))
				this._logDoc = true;
			if (log.includes('sql'))
				this._logSql = true;
		}

		this._object = { };
	}

	_alias(el) {
		let arr = el.split('~');
		return arr.length > 1 ? { [arr[1]]: arr[0] } : el;
	}

	_spread(method, values) {
		if (Array.isArray(values))
			method(...values);
		else
			method(values);
	}

	query(query = null, values = null) {
		if (query && typeof (query.is) == 'function') {
			let req = query;
			let id = parseInt(req.params.id);

			this.table(query.params.table.split('|').map(this._alias));

			if (req.method == 'POST') {
				if (!isNaN(id) && id > 0)												// UPDATE BY ID
					this.update(req.body).where({ _id: id });
				else if (req.params.id == '*')											// UPDATE WHERE
					this.update(req.body.update).where(req.body.where);
				else if (!isNaN(id) && id < 0)											// DELETE BY ID
					this.delete().where({ _id: Math.abs(id) });
				else if (req.params.id == '-')											// DELETE WHERE
					this.delete().where(req.body.where);
				else if (req.params.id)													// INSERT: +, 0
					this.insert(...(Array.isArray(req.body) ? req.body : [req.body]));
				else
					this._object.query = {
						doc: new QueryChain()
							.table(this._object.table)
							.select(req.body.select)
							.where(req.body.where)
							.order(req.body.order)
							.limit(req.body.limit)
							.offset(req.body.offset),
						len: () => {
							//
//							console.log('body', req.body);

							return req.body.count || req.body.len
								? new QueryChain()
									.table(this._object.table)
									.select({ $: 'count', count: 'COUNT(*)' })
									.where(req.body.where)
									.limit(0)
								: null;
						}
					};
			} else {
				if (!isNaN(id))														// SELECT BY ID
					this.where({ ...req.query, _id: id }).limit(0);
				else if (req.params.id)												// SELECT COLUMNS
					this.select(req.params.id.split('|').map(this._alias)).where(req.query);
				else																// SELECT ALL
					this.where(req.query);
			}
		} else if (query && Array.isArray(query)) {
			if (query.every(el => typeof(el) == 'string' && !el.includes(' ')))
				this._object.table = query;
			else
				this._object.query = query;//'BEGIN;\n' + query.map(obj => (obj instanceof QueryChain ? obj : new QueryChain().query(obj, values)).build()).join('; ') + ';\nCOMMIT';
			this._object.queryValues = values;
		} else if (query && query.endsWith('.sql')) {
			this._object.query = fs.readFileSync(query, 'utf8');
			this._object.queryValues = values;
		} else if (query && query.includes(' ')) {
			this._object.query = query;
			this._object.queryValues = values;
		} else if (values && values instanceof Object) {
			this.table(query);

			if (values.select)
				this._spread(this.select.bind(this), values.select);
			else if (values.insert)
				this._spread(this.insert.bind(this), values.insert);
			else if (values.update)
				this._spread(this.update.bind(this), values.update);
			else if (values.delete)
				this.delete();

			if (values.table)
				this.table(values.table);
			if (values.where)
				this._spread(this.where.bind(this), values.where);
			if (values.group)
				this._spread(this.group.bind(this), values.group);
			if (values.distinct)
				this._spread(this.distinct.bind(this), values.distinct);
			if (values.order)
				this._spread(this.order.bind(this), values.order);
			if (values.limit)
				this.limit(values.limit);
			if (values.offset)
				this.offset(values.offset);
			if (values.first)
				this.limit(0);
		} else {
			this._object.table = query;
//			this._object.select = '*';
		}

		return this;
	}

	table(table) {
		this._object.table = table;

		return this;
	}

	_table() {
		return Array.isArray(this._object.table)
			? this._object.table.map(this._alias).join(', ')
			: this._object.table instanceof Object
				? '(' + new QueryChain().query(null, this._object.table).build() + ') query'
				: this._object.table;
	}

	select(...cols) {
		this._object.select = cols && cols.every(el => el) ? cols : [ ];

		return this;
	}

	_select() {
		let cols = this._object.select;

		if (cols.length == 0)
			return "*";
		else
			return cols.map(obj => {
				return Array.isArray(obj)
					? obj.map(val => {
						if (val instanceof QueryChain)
							val = `(${val.build()})`;
						else if (val instanceof Object)
							val = `(${new QueryChain().query(val.table, val).build()})`;
						else if (typeof(val) == 'string' && !val.match(/\W/))
							val = `"${val}"`;

						return `${val}`;
					}).join(', ')
					: obj instanceof Object
						? Object.keys(obj).filter(key => key != '_').map(key => {
							let val = obj[key];

							if (key == '$' && obj[val] !== undefined)
								return undefined;

							if (val instanceof QueryChain)
								val = `(${val.build()})`;
							else if (val instanceof Object)
								val = `(${new QueryChain().query(val.table, val).build()})`;
							else if (typeof(val) == 'string' && !val.match(/\W/))
								val = `"${val}"`;

							return key == '$' ? val : `${val} AS ${key}`;
						}).filter(el => el !== undefined).join(', ')
						: typeof(obj) == 'string' && !obj.match(/\W/) ? `"${obj}"` : obj;
			}).join(', ');
	}

	insert(...rows) {
		this._object.insert = rows && rows.every(el => el) ? rows : null;

		return this;
	}

	_insert() {
		let rows = this._object.insert;

		let keys = [];
		rows.forEach(obj => {
			if (obj instanceof Object)
				Object.keys(obj).forEach(key => {
					if (keys.indexOf(key) < 0)
						keys.push(key);
				});
		});

		let vals = rows.map(obj => {
			return obj instanceof Object ? keys.map(key => {
				let val = obj[key];

				if (val == undefined)
					val = 'DEFAULT';
				else if (val == null)
					val = 'NULL';
				else if (typeof(val) == 'string' && val != 'NOW()' && !val.match(/^\$\d+\./))
					val = `'${val.replace(/\'/g, '\'\'')}'`;
				else if (val instanceof Object && Object.keys(val).includes('$'))
					val = val.$;
				else if (val instanceof QueryChain)
					val = `(${val.build()})`;
				else if (val instanceof Object)
					val = `(${new QueryChain().query(null, val).build()})`;

				return val;
			}).join(', ') : obj;
		});

		return keys.length > 0
			? `(${keys.map(key => `"${key}"`).join(', ')}) VALUES (${vals.join('), (')})`
			: vals.join(', ');
	}

	update(...vals) {
		this._object.update = vals && vals.every(el => el) ? vals : null;

		return this;
	}

	_update() {
		let vals = this._object.update;

		return vals.map(obj => {
			return obj instanceof Object ? Object.keys(obj).map(key => {
				let val = obj[key];

				if (val == null)
					val = 'NULL'
				else if (typeof(val) == 'string' && val != 'NOW()' && !val.match(/^\$\d+\./))
					val = `'${val.replace(/\'/g, '\'\'')}'`;
				else if (val instanceof Object && Object.keys(val).includes('$'))
					val = val.$;
				else if (val instanceof QueryChain)
					val = `(${val.build()})`;
				else if (val instanceof Object)
					val = `(${new QueryChain().query(null, val).build()})`;

				return `${key}=${val}`;
			}).join(', ') : obj;
		}).join(', ');
	}

	delete() {
		this._object.delete = '*';

		return this;
	}

	where(...where) {
		this._object.where = where && where.every(el => el) ? where : null;

		return this;
	}

	_where() {
		let where = this._object.where;
		if (!where)
			return null;

		let proc = where => where.map(arg => {
			if (Array.isArray(arg))
				return arg.length ? '(' + proc(arg).join(' OR ') + ')' : null;
			else if (arg instanceof Object)
				return Object.keys(arg).map(key => {
					var val = arg[key];

					if (key == '$') {
						return val ? '(' + proc(Array.isArray(val) ? val : [ val ]).join(' OR ') + ')' : null;
					} else if (Array.isArray(val)) {
						let arr = val.map(tmp => typeof (tmp) == 'string' && tmp != 'NOW()' && !tmp.match(/^\$\d+\./)
							? `'${tmp.replace(/\'/g, '\'\'')}'`
							: tmp instanceof Object && Object.keys(tmp).includes('$')
								? tmp.$
								: tmp);

						return arr.length > 0 ? `${key} IN ( ${arr.join(', ')} )` : 'FALSE';
					} else if (val instanceof Object) {
						if (Object.keys(val).includes('$'))
							return `${key} = ${val.$}`;

						let tmp = val instanceof QueryChain ? val : new QueryChain().query(null, val).build();

						return val.limit == 1 || val.first == 1 ? `${key} = (${tmp})` : `${key} IN (${tmp})`
					}

					/*if (val == null)
						val = 'NULL';
					else */if (typeof(val) == 'string' && val != 'NOW()' && !val.match(/^\$\d+\./))
						val = `'${val.replace(/\'/g, '\'\'')}'`;
					else if (val instanceof Object && Object.keys(val).includes('$'))
						val = val.$;
					else if (val instanceof QueryChain)
						val = `(${val.build()})`;

					return val == null ? `${key} IS NULL` : `${key} = ${val}`;
				}).join(' AND ');
			else
				return arg;
		});
		let args = proc(where);

		return args.join(' OR ');
	}

	group(...group) {
		this._object.group = group && group.every(el => el) ? group : null;

		return this;
	}

	_group() {
		let group = this._object.group;
		if (!group)
			return null;

		return group.join(', ');
	}

	distinct(...distinct) {
		this._object.distinct = distinct && distinct.every(el => el) ? distinct : null;

		return this;
	}

	_distinct() {
		let distinct = this._object.distinct;
		if (!distinct)
			return null;

		return distinct.join(', ');
	}

	order(...order) {
		this._object.order = order && order.every(el => el) ? order : null;

		return this;
	}

	_order() {
		let order = this._object.order;
		if (!order)
			return null;

		let args = order.map(arg => {
			if (arg instanceof Object)
				return Object.keys(arg).map(key => {
					var val = arg[key];

					if (typeof(key) == 'string' && !key.match(/\W/))
						key = `"${key}"`;

					if (typeof(val) == 'number')
						val = val > 0 ? 'ASC' : 'DESC';

					return `${key} ${val}`;
				}).join(', ');
			else
				return arg;
		});

		return args.join(', ');
	}

	limit(limit) {
		if (limit === 0)
			this._object.first = 1;
		else
			this._object.limit = limit;

		return this;
	}

	offset(offset) {
		this._object.offset = offset;

		return this;
	}

	conflict(...conflict) {
		this._object.conflict = conflict && conflict.every(el => el) ? conflict : null;

		return this;
	}

	_conflict() {
		let conflict = this._object.conflict;
		if (!conflict)
			return null;

		return conflict.join(', ');
	}

	count(col) {
		this._object.count = col || '*';

		return this;
	}

	_count() {
		return this._object.count == '*'
			? 'COUNT(*)'
			: `COUNT("${this._object.count}")`;
	}

	/*
	WARNING: Switch first to fetching
	*/
	first(callback, db = null) {
		this._object.first = 1;

//		if (!callback)
//			return this;
//		else
			return this.fetch(callback, db);
	}

	createDatabase(database) {
		this._object.createDatabase = database;

		return this;
	}

	dropDatabase(database) {
		this._object.dropDatabase = database;

		return this;
	}

	createUser(username, password) {
		this._object.query = `CREATE USER ${username} WITH ENCRYPTED PASSWORD '${password}'`;

		return this;
	}

	dropUser(username) {
		this._object.query = `DROP USER ${username}`;

		return this;
	}

	grantPrivileges(to, on, privileges = undefined, onModifier = undefined) {
		this._object.query = `GRANT ${privileges ? privileges.join(', ') : 'ALL'} PRIVILEGES ON ${onModifier} ${on} TO ${to}`;

		return this;
	}

	columns(...cols) {
		this._object.columns = cols && cols.every(el => el) ? cols : [];

		return this;
	}

	_columns() {
		let cols = this._object.columns;

		return cols.length
			? cols.map(obj => {
				return obj instanceof Object
					? Object.keys(obj).map(key => {
						let val = obj[key];

						if (typeof(val) == 'string' && !val.match(/\W/))
							val = `"${val}"`;

						return key == '$' ? val : `${val} AS ${key}`;
					}).join(', ')
					: typeof (obj) == 'string' && !obj.match(/\W/)
						? `"${obj}"`
						: obj;
			}).join(', ')
			: '*';
	}

	// WARNING: addColumn
	addColumn(...cols) {
		this._object.addColumn = cols/*.map(col => {
			return col;
		})*/.join(', ');

		return this;
	}

	// WARNING: dropColumn
	dropColumn(...cols) {
		this._object.addColumn = cols/*.map(col => {
			return col;
		})*/.join(', ');

		return this;
	}

	begin() {
		this._object.query = 'BEGIN';

		return this;
	}

	commit() {
		this._object.query = 'COMMIT';

		return this;
	}

	rollback() {
		this._object.query = 'ROLLBACK';

		return this;
	}

	batch(callback, db = null) {
		(db || this._db).connect((err, client, done) => {
			var tran = 0;

			const exit = (err, docs, sqls) => {
				if (callback)
					(Array.isArray(callback) ? callback : [callback]).forEach(callback => {
						let isObject = this._object.query instanceof Object && !Array.isArray(this._object.query);

						if (isObject)
							docs = Object.keys(this._object.query).reduce((prev, curr, index) => {
								if (docs.length > index)
									prev[curr] = docs[index];
								return prev;
							}, {});

						if (typeof (callback.send) == 'function')
							callback.send(isObject
								? { ...docs, err: err ? err : undefined, msg: err ? err.toString() : undefined }
								: { doc: docs, err: err ? err : undefined, msg: err ? err.toString() : undefined });
						else
							callback(err, docs, sqls);
					});

				if (done)
					done();
			};

			const next = (err, docs, sqls, idx) => {
				let arr = Array.isArray(this._object.query) ? this._object.query : Object.values(this._object.query);

				if (err) {
					if (tran)
						new QueryChain().rollback().fetch(() => {
							exit(err, docs, sqls);
						}, client);
					else
						exit(err, docs, sqls);
				} else if (idx < arr.length) {
					let obj = arr[idx];

					if (typeof(obj) == 'function')
						obj = obj(docs, err, sqls);

					if (obj) {
						(obj instanceof QueryChain ? obj : new QueryChain().query(obj, this._object.queryValues)).fetch((err, doc, sql) => {
							if (obj._object.query == 'BEGIN')
								tran++;
							else if (obj._object.query == 'COMMIT' || obj._object.query == 'ROLLBACK')
								tran--;

							docs.push(doc);
							sqls.push(sql);

							next(err, docs, sqls, idx + 1);
						}, client);
					} else {
						docs.push(null);
						sqls.push(null);

						next(err, docs, sqls, idx + 1);
					}
				} else {
					if (tran)
						new QueryChain().commit().fetch(err => {
							exit(err, docs, sqls);
						}, client);
					else
						exit(err, docs, sqls);
				}
			};

//			new QueryChain().begin().fetch(err => {
				next(null/*err*/, [ ], [ ], 0);
//			}, client);
		});
	}

	build() {
		if (this._object.query)
			return this._object.query;

		this._object.queryValues = null;

		var sql = null;

		if (this._object.insert) {
			sql = `INSERT INTO ${this._table()} ${this._insert()}`;

			if (this._object.conflict)
				sql += ` ON CONFLICT (${this._conflict()}) DO ${
					this._object.update
						? 'UPDATE SET ' + this._update()
						: 'NOTHING'
				}`;

			return sql;
		} else if (this._object.update)
			sql = `UPDATE ${this._table()} SET ${this._update()}`;
		else if (this._object.delete)
			sql = `DELETE FROM ${this._table()}`;
		else if (this._object.createDatabase)
			return `CREATE DATABASE ${this._object.createDatabase}`;
		else if (this._object.dropDatabase)
			return `DROP DATABASE IF EXISTS ${this._object.dropDatabase}`;
		else if (this._object.columns)
			return `SELECT ${this._columns()}
					FROM information_schema.columns
					WHERE table_name = '${this._table()}'`;
		else if (this._object.addColumn)
			return `ALTER TABLE ${this._table()} ${this._object.addColumn}`;
		else if (this._object.dropColumn)
			return `ALTER TABLE ${this._table()} ${this._object.dropColumn}`;
		else {
			sql = 'SELECT';

			let distinct = this._distinct();
			if (distinct)
				sql += ` DISTINCT ON (${distinct})`;

			sql += this._object.table
				? ` ${this._object.select
					? this._select()
					: '*'} FROM ${this._table()}`
				: ` ${this._object.select ? this._select() : '*'}`;
		}

		let where = this._where();
		if (where)
			sql += ` WHERE ${where}`;
		let group = this._group();
		if (group)
			sql += ` GROUP BY ${group}`;
		let order = this._order();
		if (order)
			sql += ` ORDER BY ${order}`;
		if (this._object.limit)
			sql += ` LIMIT ${this._object.limit}`;
		else if (this._object.first)
			sql += ` LIMIT 1`;
		if (this._object.offset)
			sql += ` OFFSET ${this._object.offset}`;

		if (Array.isArray(this._object.table))
			this._object.table.forEach((table, index) => {
				sql = sql.replace(new RegExp(`\\$${index + 1}\\.`, 'g'), `${table}.`);
			});

		return sql;
	}

	fetch(callback, db = null) {
		if (!callback)
			return this.execute(db);

		if (this._object.count) {
			if (this._object.select)
				this._object.query = {
					doc: new QueryChain()
						.query(null, { ...this._object, count: undefined }),
					len: new QueryChain()
						.query(null, { table: this._object.table, where: this._object.where })
						.select({ $: 'count', count: this._count() })
						.limit(0)
				};
			else
				this.select({ $: 'count', count: this._count() });
		}

		if (Array.isArray(this._object.query) || this._object.query instanceof Object)
			return this.batch(callback, db);

		let sql = this.build();

		if (this._logSql)
			console.log(sql);

		if (db == null && this._db == null) {
			if (callback)
				if (typeof(callback.end) == 'function')
					callback.end();
				else
					callback();
		} else {
			(db || this._db).query(sql, this._object.queryValues, (err, doc) => {
				if (doc) {
					if (/*this._object.select*/doc.rows && Object.keys(doc.rows).length == doc.rowCount) {
						let cols = this._object.select || this._object.columns;
						let col = cols
							? cols.find(el => el instanceof Object && typeof (el.$) == 'string')
							: null;
						let rows = col
							? cols.length > 1
								? doc.rows.reduce((prev, curr) => {
									prev[curr[col.$]] = col._ ? Array.isArray(col._) ? col._.reduce((p, c) => {
										p[c] = curr[c];
										return p;
									}, {}) : curr[col._] : curr;
									return prev;
								}, {})
								: doc.rows.map(row => row[col.$])
							: doc.rows;

						doc = this._object.first == 1 ? doc.rowCount > 0 ? rows[0] : null : rows;
					} else if (this._object.createDatabase)
						doc = this._object.createDatabase;
					else if (this._object.dropDatabase)
						doc = this._object.dropDatabase;
					else
						doc = doc.rowCount;
				}

				if (this._logErr && err) {
					if (!this._logSql)
						console.log(sql);

					console.log(err.toString());
				} else if (this._logDoc) {
					console.log(doc);
				}

				if (callback)
					(Array.isArray(callback) ? callback : [callback]).forEach(callback => {
						if (typeof (callback.send) == 'function')
							callback.send({ doc: doc, err: err, msg: err ? err.toString() : undefined });
						else if (typeof (callback) == 'function')
							callback(err, doc, sql);
					});
			});
		}
	}

	/*
	WARNING: Switch execute to fetch
	*/
	execute(db = null) {
		return new Promise((resolve, reject) => {
			this.fetch((err, doc) => {
				if (err)
					reject(err);
				else
					resolve(doc);
			}, db);
		});
	}

	print(db = null) {
		if (db == null && this._db == null)
			console.log(this.build());
		else
			this.fetch(db || this._db, (err, doc, sql) => {
				console.log({ doc: doc, err: err.stack, sql: sql });
			});
	}

	toString() {
		return this.build();
	}

	serialize() {
		return this._object;
	}
}
