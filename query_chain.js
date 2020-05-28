const QueryBuild = require('./query_build.js')
const fs = require('fs');

module.exports = class QueryChain extends QueryBuild {
	constructor(db = null, log = null) {
		super(log);

		this._db = db;
	}

	query(query = null, values = null) {
		if (query && typeof (query.is) == 'function' && values && values.$) {
			let req = query;
			let key = values.$;
			let val = values._;
			if (val) {
				let setting = req.params.id;
				let other = Object.keys(values).filter(el => ![ '$', '_' ].includes(el)).reduce((prev, curr) => {
					prev[curr] = values[curr];

					return prev;
				}, {});

				this.table(req.params.table);

				if (req.method == 'POST') {
					if (setting) {
						if (req.body != null) {
							let str = JSON.stringify(req.body);

							this.insert({ ...other, [key]: setting, [val]: str })
								.conflict(...Object.keys(other), key)
								.update({ [val]: str });
						} else {
							this.delete().where({ ...other, [key]: setting });
						}
					} else {
						let columns = Object.keys(req.body);

						this.query([
							new QueryChain(null, this._log).begin(),

							new QueryChain(null, this._log)
								.table(req.params.table)
								.delete()
								.where({ ...other, [key]: columns }),
							new QueryChain(null, this._log)
								.table(req.params.table)
								.insert(...columns.filter(el => req.body[el] != null).map(el => {
									return { ...other, [key]: el, [val]: JSON.stringify(req.body[el]) };
								}))
						]);
					}
				} else {
					if (setting) {
						let settings = setting.split('|');

						if (settings.length > 1)
							this.select({ $: key, _: val }, val).where({ ...other, [key]: settings });
						else
							this.select({ $: val }).where({ ...other, [key]: setting }).limit(0);
					} else {
						this.select({ $: key, _: val }, val);
					}
				}
			} else {
				let id = parseInt(req.params.id);

				this.table(query.params.table.split('|').map(this._alias));

				if (req.method == 'POST') {
					if (!isNaN(id) && id > 0)												// UPDATE BY ID
						this.update(req.body).where({ [key]: id });
					else if (req.params.id == '*')											// UPDATE WHERE
						this.update(req.body.update).where(req.body.where);
					else if (!isNaN(id) && id < 0)											// DELETE BY ID
						this.delete().where({ [key]: Math.abs(id) });
					else if (req.params.id == '-')											// DELETE WHERE
						this.delete().where(req.body.where);
					else if (req.params.id)													// INSERT: +, 0
						this.insert(...(Array.isArray(req.body) ? req.body : [req.body]));
					else
						this._object.query = {
							doc: new QueryChain(null, this._log)
								.table(this._object.table)
								.select(req.body.select)
								.where(req.body.where)
								.order(req.body.order)
								.limit(req.body.limit)
								.offset(req.body.offset),
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
				} else {
					if (!isNaN(id))														// SELECT BY ID
						this.where({ ...req.query, [key]: id }).limit(0);
					else if (req.params.id)												// SELECT COLUMNS
						this.select(req.params.id.split('|').map(this._alias)).where(req.query);
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
						else if (isObject && this._object.count)
							callback(err, docs.doc, docs.len, sqls);
						else
							callback(err, docs, undefined, sqls);
					});

				if (done)
					done();
			};

			const next = (err, docs, sqls, idx) => {
				let arr = Array.isArray(this._object.query) ? this._object.query : Object.values(this._object.query);

				if (err) {
					if (tran)
						new QueryChain(null, this._log).rollback().fetch(() => {
							exit(err, docs, sqls);
						}, client);
					else
						exit(err, docs, sqls);
				} else if (idx < arr.length) {
					let obj = arr[idx];

					if (typeof(obj) == 'function')
						obj = obj(docs, err, sqls);

					if (obj) {
						(obj instanceof QueryChain ? obj : new QueryChain(null, this._log).query(obj, this._object.queryValues)).fetch((err, doc, sql) => {
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
						new QueryChain(null, this._log).commit().fetch(err => {
							exit(err, docs, sqls);
						}, client);
					else
						exit(err, docs, sqls);
				}
			};

//			new QueryChain(null, this._log).begin().fetch(err => {
				next(null/*err*/, [ ], [ ], 0);
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
							callback(err, doc, undefined, sql);
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
