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
	}

	query(query = null, values = null) {
		if (query && Array.isArray(query)) {
			if (query.every(el => typeof(el) == 'string' && !el.includes(' ')))
				this._table = query;
			else
				this._query = query;//'BEGIN;\n' + query.map(obj => (obj instanceof QueryChain ? obj : new QueryChain().query(obj, values)).build()).join('; ') + ';\nCOMMIT';
			this._queryValues = values;
		} else if (query && query.endsWith('.sql')) {
			this._query = fs.readFileSync(query, 'utf8');
			this._queryValues = values;
		} else if (query && query.includes(' ')) {
			this._query = query;
			this._queryValues = values;
		} else if (values && values instanceof Object) {
			this.table(query);

			const spread = (method, values) => {
				if (Array.isArray(values))
					method(...values);
				else
					method(values);
			}

			if (values.select)
				spread(this.select.bind(this), values.select);
			else if (values.insert)
				spread(this.insert.bind(this), values.insert);
			else if (values.update)
				spread(this.update.bind(this), values.update);
			else if (values.delete)
				this.delete();

			if (values.table)
				this.table(values.table);
			if (values.where)
				spread(this.where.bind(this), values.where);
			if (values.group)
				spread(this.group.bind(this), values.group);
			if (values.order)
				spread(this.order.bind(this), values.order);
			if (values.limit)
				this.limit(values.limit);
		} else {
			this._table = query;
//			this._select = '*';
		}

		return this;
	}

	table(table) {
		this._table = table;

		return this;
	}

	select(...cols) {
		if (cols.length == 0)
			this._select = "*";
		else
			this._select = cols.map(obj => {
				return obj instanceof Object ? Object.keys(obj).map(key => {
					let val = obj[key];

					if (val instanceof QueryChain)
						val = `(${val.build()})`;
					else if (val instanceof Object)
						val = `(${new QueryChain().query(val.table, val).build()})`;

					return `${val} AS ${key}`;
				}).join(', ') : obj;
			}).join(', ');

		return this;
	}

	insert(...rows) {
		if (rows.length == 0)
			return this;

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
					val = `'${val.replace('\'', '\'\'')}'`;
				else if (val instanceof QueryChain)
					val = `(${val.build()})`;
				else if (val instanceof Object)
					val = `(${new QueryChain().query(null, val).build()})`;

				return val;
			}).join(', ') : obj;
		});

		this._insert = keys.length > 0
			? `(${keys.join(', ')}) VALUES (${vals.join('), (')})`
			: vals.join(', ');

		return this;
	}

	update(...vals) {
		if (vals.length == 0)
			return this;

		this._update = vals.map(obj => {
			return obj instanceof Object ? Object.keys(obj).map(key => {
				let val = obj[key];

				if (val == null)
					val = 'NULL'
				else if (typeof(val) == 'string' && val != 'NOW()' && !val.match(/^\$\d+\./))
					val = `'${val.replace('\'', '\'\'')}'`;

				return `${key}=${val}`;
			}).join(', ') : obj;
		}).join(', ');

		return this;
	}

	delete() {
		this._delete = '*';

		return this;
	}

	where(...where) {
		let args = where.map(arg => {
			if (arg instanceof Object)
				return Object.keys(arg).map(key => {
					var val = arg[key];

					if (Array.isArray(val)) {
						let arr = val.map(tmp => typeof(tmp) == 'string' && val != 'NOW()' && !val.match(/^\$\d+\./) ? `'${tmp.replace('\'', '\'\'')}'` : tmp);

						return arr.count > 0 ? `${key} IN ( ${arr.join(', ')} )` : 'FALSE';
					} else if (val instanceof Object) {
						let tmp = val instanceof QueryChain ? val : new QueryChain().query(null, val).build();

						return val.limit == 1 ? `${key} = (${tmp})` : `${key} IN (${tmp})`
					}

					if (val == null)
						val = 'NULL';
					else if (typeof(val) == 'string' && val != 'NOW()' && !val.match(/^\$\d+\./))
						val = `'${val.replace('\'', '\'\'')}'`;
					else if (val instanceof QueryChain)
						val = `(${val.build()})`;

					return `${key} = ${val}`;
				}).join(' AND ');
			else
				return arg;
		});

		this._where = args.join(' OR ');

		return this;
	}

	group(...group) {
		this._group = group.join(', ');

		return this;
	}

	order(...order) {
		let args = order.map(arg => {
			if (arg instanceof Object)
				return Object.keys(arg).map(key => {
					var val = arg[key];

					if (typeof(val) == 'number')
						val = val > 0 ? 'ASC' : 'DESC';

					return `${key} ${val}`;
				}).join(', ');
			else
				return arg;
		});

		this._order = args.join(', ');

		return this;
	}

	limit(limit) {
		this._limit = limit;

		return this;
	}

	createDatabase(database) {
		this._createDatabase = database;

		return this;
	}

	dropDatabase(database) {
		this._dropDatabase = database;

		return this;
	}

	// WARNING: addColumn
	addColumn(...cols) {
		this._addColumn = cols.map(col => {
			return col;
		}).join(', ');

		return this;
	}

	// WARNING: dropColumn
	dropColumn(...cols) {
		this._addColumn = cols.map(col => {
			return col;
		}).join(', ');

		return this;
	}

	begin() {
		this._query = 'BEGIN';

		return this;
	}

	commit() {
		this._query = 'COMMIT';

		return this;
	}

	rollback() {
		this._query = 'ROLLBACK';

		return this;
	}

	batch(callback, db = null) {
		(db || this._db).connect((err, client, done) => {
			var tran = 0;

			const exit = (err, docs, sqls) => {
				if (callback)
					callback(err, docs, sqls);

				done();
			};

			const next = (err, docs, sqls, idx) => {
				if (err) {
					if (tran)
						new QueryChain().rollback().fetch(() => {
							exit(err, docs, sqls);
						}, client);
					else
						exit(err, docs, sqls);
				} else if (idx < this._query.length) {
					let obj = this._query[idx];

					if (typeof(obj) == 'function')
						obj = obj(docs, err, sqls);

					if (obj)
						(obj instanceof QueryChain ? obj : new QueryChain().query(obj, this._queryValues)).fetch((err, doc, sql) => {
							if (obj._query == 'BEGIN')
								tran++;
							else if (obj._query == 'COMMIT' || obj._query == 'ROLLBACK')
								tran--;

							docs.push(doc);
							sqls.push(sql);

							next(err, docs, sqls, idx + 1);
						}, client);
					else
						next(err, docs, sqls, idx + 1);
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
		if (this._query)
			return this._query;

		this._queryValues = null;

		var sql = null;

		if (this._insert)
			return `INSERT INTO ${this._table} ${this._insert}`;
		else if (this._update)
			sql = `UPDATE ${this._table} SET ${this._update}`;
		else if (this._delete)
			sql = `DELETE FROM ${this._table}`;
		else if (this._createDatabase)
			return `CREATE DATABASE ${this._createDatabase}`;
		else if (this._dropDatabase)
			return `DROP DATABASE IF EXISTS ${this._dropDatabase}`;
		else if (this._addColumn)
			return `ALTER TABLE ${this._table} ${this._addColumn}`;
		else if (this._dropColumn)
			return `ALTER TABLE ${this._table} ${this._dropColumn}`;
		else
			sql = this._table
				? `SELECT ${this._select ? this._select : '*'} FROM ${Array.isArray(this._table) ? this._table.join(', ') : this._table}`
				: `SELECT ${this._select ? this._select : '*'}`;

		if (this._where)
			sql += ` WHERE ${this._where}`;
		if (this._group)
			sql += ` GROUP BY ${this._group}`;
		if (this._order)
			sql += ` ORDER BY ${this._order}`;
		if (this._limit)
			sql += ` LIMIT ${this._limit}`;

		if (Array.isArray(this._table))
			this._table.forEach((table, index) => {
				sql = sql.replace(new RegExp(`\\$${index + 1}\\.`, 'g'), `${table}.`);
			});

		return sql;
	}

	fetch(callback, db = null) {
		if (Array.isArray(this._query))
			return this.batch(callback, db);

		let sql = this.build();

		if (this._logSql)
			console.log(sql);

		if (db == null && this._db == null) {
			if (callback)
				callback();
		} else {
			(db || this._db).query(sql, this._queryValues, (err, doc) => {
				if (doc) {
					if (/*this._select*/doc.rows && Object.keys(doc.rows).length == doc.rowCount)
						doc = doc.rowCount > 0 ? this._limit == 1 ? doc.rows[0] : doc.rows : null;
					else if (this._createDatabase)
						doc = this._createDatabase;
					else if (this._dropDatabase)
						doc = this._dropDatabase;
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
					callback(err, doc, sql);
			});
		}
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
}
