const QueryBuild = require('./query_build.js')
const fs = require('fs');

module.exports = class QueryChain extends QueryBuild {
	constructor(db = null, log = null, err = null) {
		super(log);

		this._db = db;
		this._err = err;
	}

	query(query = null, values = null) {
		if (query && typeof (query.is) == 'function' && values && values.$) {
			let req = query;
			let key = values.$;
			let val = values._;
			let json = val && val.includes('json');

			let table = req.params.table;
			let param = req.params.id;// || req.params[0];
			if (val) {
				let other = Object.keys(values).filter(el => ![ '$', '_' ].includes(el)).reduce((prev, curr) => {
					prev[curr] = values[curr];

					return prev;
				}, {});

				this.table(table);

				if (req.method == 'POST') {
					if (param) {
						if (req.body != null) {
							let str = json ? JSON.stringify(req.body) : req.body;

							this.insert({ ...other, [key]: param, [val]: str })
								.conflict(...Object.keys(other), key)
								.update({ [val]: str });
						} else {
							this.delete().where({ ...other, [key]: param });
						}
					} else {
						let columns = Object.keys(req.body);

						let del = columns.filter(el => req.body[el] == null);
						let ins = columns.filter(el => req.body[el] != null);
						this.query([
//							new QueryChain(null, this._log).begin(),

							() => del.length
								? new QueryChain(null, this._log)
									.table(table || this._object.table)
									.delete()
									.where({ ...other, [key]: del })
								: null,
							() => ins.length
								? new QueryChain(null, this._log)
									.table(table || this._object.table)
									.insert(...ins.map(el => {
										return { ...other, [key]: el, [val]: json ? JSON.stringify(req.body[el]) : req.body[el] };
									}))
									.conflict(...Object.keys(other), key)
									.update({ [val]: { $: `excluded.${val}` } })
								: null
						]);
					}
				} else {
					if (param) {
						let settings = param.split('|');

						if (settings.length > 1)
							this.select({ $: key, _: val }, val).where({ ...other, [key]: settings });
						else
							this.select({ $: val }).where({ ...other, [key]: param }).limit(0);
					} else {
						this.select({ $: key, _: val }, val).where(other);
					}
				}
			} else {
				let id = parseInt(param);

				this.table(table.split('|').map(this._alias));

				if (req.method == 'POST') {
					if (!isNaN(id) && id > 0)												// UPDATE BY ID
						this.update(req.body).where({ [key]: id });
					else if (param == '*')													// UPDATE WHERE
						this.update(req.body.update).where(req.body.where);
					else if (!isNaN(id) && id < 0)											// DELETE BY ID
						this.delete().where({ [key]: Math.abs(id) });
					else if (param == '-')													// DELETE WHERE
						this.delete().where(req.body.where);
					else if (param)															// INSERT: +, 0
						this.insert(...(Array.isArray(req.body) ? req.body : [req.body]));
					else {
						var sql = new QueryChain(null, this._log);
						sql = req.body.insert
							? sql.insert(req.body.insert)
							: req.body.update
								? sql.update(req.body.update)
								: req.body.delete
									? sql.delete(req.body.delete)
									: sql.select(req.body.select);
						this._object.query = {
							doc: sql
								.table(this._object.table)
//								.select(req.body.select)
								.where(req.body.where)
								.order(req.body.order)
								.limit(req.body.limit)
								.offset(req.body.offset)
								.distinct(req.body.distinct),
							len: () => {
//
//								console.log('body', req.body);

								return req.body.count || req.body.len
									? new QueryChain(null, this._log)
										.table(this._object.table)
										.select({ $: 'count', count: 'COUNT(*)' })
										.where(req.body.where)
										.limit(0)
									: null;
							}
						};
					}
				} else {
					if (!isNaN(id))														// SELECT BY ID
						this.where({ ...req.query, [key]: id }).limit(0);
					else if (param)														// SELECT COLUMNS
						this.select(param.split('|').map(this._alias)).where(req.query);
					else																// SELECT ALL
						this.where(req.query);
				}
			}
		} else if (/*query*/typeof query == 'string' && query.endsWith('.sql')) {
			this._object.query = fs.readFileSync(query, 'utf8');
			this._object.queryValues = values;
		} else {
			super.query(query, values);
		}

		return this;
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

	batch(callback, db = null) {
		(db || this._db).connect((err, client, done) => {
			var tran = 0;

			const exit = (err, docs, lens, sqls) => {
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
								: { doc: docs, len: lens, err: err ? err : undefined, msg: err ? err.toString() : undefined });
						else if (isObject && this._object.count)
							callback(err, docs.doc, docs.len, sqls);
						else
							callback(err, docs, lens, sqls);
					});

				if (done)
					done();
			};

			const next = (err, docs, lens, sqls, idx) => {
				let arr = Array.isArray(this._object.query) ? this._object.query : Object.values(this._object.query);

				if (err) {
					if (tran)
						new QueryChain(null, this._log).rollback().fetch(() => {
							exit(err, docs, lens, sqls);
						}, client);
					else
						exit(err, docs, lens, sqls);
				} else if (idx < arr.length) {
					let obj = arr[idx];

					if (typeof(obj) == 'function')
						obj = obj(docs, /*lens, */err, sqls);

					if (obj) {
						(obj instanceof QueryChain ? obj : new QueryChain(null, this._log).query(obj, this._object.queryValues)).fetch((err, doc, len, sql) => {
							if (obj._object.query == 'BEGIN')
								tran++;
							else if (obj._object.query == 'COMMIT' || obj._object.query == 'ROLLBACK')
								tran--;

							docs.push(doc);
							lens.push(len);
							sqls.push(sql);

							next(err, docs, lens, sqls, idx + 1);
						}, client);
					} else {
						docs.push(null);
						lens.push(null);
						sqls.push(null);

						next(err, docs, lens, sqls, idx + 1);
					}
				} else {
					if (tran)
						new QueryChain(null, this._log).commit().fetch(err => {
							exit(err, docs, lens, sqls);
						}, client);
					else
						exit(err, docs, lens, sqls);
				}
			};

//			new QueryChain(null, this._log).begin().fetch(err => {
				next(null/*err*/, [ ], [ ], [ ], 0);
//			}, client);
		});
	}

	fetch(callback, db = null) {
		if (!callback)
			return this.execute(db);

		if (this._object.count) {
//			if (this._object.select)
				this._object.query = {
					doc: new QueryChain(null, this._log)
						.query(null, { ...this._object, count: undefined }),
					len: new QueryChain(null, this._log)
						.query(null, this._object.select && this._select().includes('(')
							? {
								table: {
									table: this._object.table,
									where: this._object.where,
									group: this._object.group,
									select: this._object.select
								}
							}
							: {
								table: this._object.table,
								where: this._object.where,
								group: this._object.group
							})
						.select({ $: 'count', count: this._count() })
						.limit(0)
				};
//			else
//				this.select({ $: 'count', count: this._count() });
		}

		if (Array.isArray(this._object.query) || this._object.query instanceof Object)
			return this.batch(callback, db);

		if (this._log.obj)
			console.log('OBJ', this._object);

		let sql = this.build();

		if (this._log.sql)
			console.log('SQL', sql);

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
						let cols = this._object.select || this._object.return || this._object.columns;
						let col = cols
							? cols.find(el => el instanceof Object && typeof (el.$) == 'string')
							: null;
						let rows = col
							? cols.length > 1
								? doc.rows.reduce((prev, curr) => {
									prev[curr[col.$]] = col._ ? Array.isArray(col._) ? col._.reduce((p, c) => {
										p[c] = curr[c];
										return p;
									}, {}) : curr[col._ instanceof Object ? Object.keys(col._)[0] : col._] : curr;
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

				if (this._log.err && err) {
					if (!this._log.sql)
						console.log('SQL', sql);

					console.log('ERR', err.toString());
				} else if (this._log.doc) {
					console.log('DOC', doc);
				}

				if (callback)
					(Array.isArray(callback) ? callback : [callback]).forEach(callback => {
						if (typeof (callback.send) == 'function')
							callback.send({ doc: doc, err: err, msg: err ? err.toString() : undefined });
						else if (typeof (callback) == 'function')
							try {
								callback(err, doc, undefined, sql);
							} catch (ex) {
								if (this._err && typeof(this._err) == 'function')
									this._err(ex);
								else
									throw ex;
							}
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
			super.print();
		else
			this.fetch(db || this._db, (err, doc, sql) => {
				console.log({ doc: doc, err: err.stack, sql: sql });
			});
	}
}
