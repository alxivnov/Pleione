const QueryBuild = require('./query_build.js');
const crypto = require('crypto');
const fs = require('fs');

module.exports = class QueryChain extends QueryBuild {
	constructor(db = null, opt = null) {
		super(opt && opt.log || opt);

		this._db = db;
		this._err = opt && opt.err;
		this._msg = opt && opt.msg;

		this._enc = opt && opt.enc;
	}

	_alias(el) {
		let arr = el.split('~');
		return arr.length > 1 ? { [arr[1]]: arr[0] } : el;
	}

	_cipher(from, password, opt) {
		if (typeof (opt) == 'string')
			opt = { algorithm: opt };

		if (!from)
			return null;

		try {
			let key = Buffer.isBuffer(password) ? password : crypto.scryptSync(password, opt && opt.salt || 'salt', opt && opt.keylen || 24);
			let iv = opt && Buffer.isBuffer(opt.iv) ? opt.iv : Buffer.alloc(16, 0);

			let decipher = opt && opt.decipher;
			let algorithm = opt && opt.algorithm || 'aes-192-cbc';
			let transform = decipher ? crypto.createDecipheriv(algorithm, key, iv) : crypto.createCipheriv(algorithm, key, iv);
			let input_encoding = opt && opt.input_encoding || (decipher ? from.match(/[^0-9a-fA-F]/) ? 'base64' : 'hex' : 'utf8');
			let output_encoding = opt && opt.output_encoding || (decipher ? 'utf8' : 'base64'/*'hex'*/);

			let to = transform.update(from, input_encoding, output_encoding);
			to += transform.final(output_encoding);

//			console.log(from, to, password);

			return to;
		} catch (err) {
			console.error(err);

			return undefined;
		}
	}

	_cipherRows(rows, decipher) {
		return this._enc && this._enc.password && rows.map(row => {
			Object.keys(row)
				.filter(key => key.toLowerCase().endsWith('__enc'))
				.filter(key => typeof (row[key]) == 'string')
				.forEach(key => {
					row[key] = this._cipher(row[key], this._enc.password, { ...this._enc, decipher });
				});

			return row;
		}) || rows;
	}

	_convertRes(res) {
		var doc = undefined;

		let cols = this._object.select || this._object.return || this._object.tables || this._object.columns;
		let col = (cols || []).reduce((col, el) => {
			if (el instanceof Object) {
				if (typeof (el.$) == 'string')
					col.$ = el.$;
				if (typeof (el._) == 'string')
					col._ = el._;
			}

			return col;
		}, {});
		var rows = this._cipherRows(res.rows, true);
		if (col.$ && (col._ || cols.length > 1)) {
			doc = rows.reduce((prev, curr) => {
				prev[curr[col.$]] = col._
					? Array.isArray(col._)
						? col._.reduce((p, c) => {
							p[c] = curr[c];
							return p;
						}, {})
						: curr[col._ instanceof Object ? Object.keys(col._)[0] : col._]
					: curr;
				return prev;
			}, {});
		} else {
			if (col.$)
				rows = rows.map(row => row[col.$]);

			doc = this._object.first == 1 ? res.rowCount > 0 ? rows[0] : null : rows;
		}

		return doc;
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
							let str = json ? JSON.stringify(req.body).replace(/'/g, `''`) : req.body;

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
										return { ...other, [key]: el, [val]: json ? JSON.stringify(req.body[el]).replace(/'/g, `''`) : req.body[el] };
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
//				let id = parseInt(param);
				let ids = param && param.split('|').map(id => parseInt(id)).filter(id => !isNaN(id)) || [];

				if (table)
					this.table(table.split('|').map(this._alias));

				if (req.method == 'POST') {
					if (/*!isNaN(id) && id > 0*/ids.length && ids.every(id => id > 0))		// UPDATE BY ID
						this.update(req.body).where({ [key]: /*id*/ids });
					else if (param == '*')													// UPDATE WHERE
						this.update(req.body.update).where({ ...req.query, ...req.body.where });
					else if (/*!isNaN(id) && id < 0*/ids.length && ids.every(id => id < 0))	// DELETE BY ID
						this.delete().where({ [key]: /*Math.abs(id)*/ids.map(id => Math.abs(id)) });
					else if (param == '-')													// DELETE WHERE
						this.delete().where({ ...req.query, ...req.body.where });
					else if (param)															// INSERT: +, 0
						this.insert(...(Array.isArray(req.body) ? req.body : [req.body]));
					else {
						let log = req.body && req.body.log
							? Object.keys(this._log).reduce((log, key) => {
								log[key] = typeof (req.body.log) == 'string'
									? req.body.log.includes(key)
									: log[key] = req.body.log[key];

								return log;
							}, this._log)
							: this._log;

						if (table) {
							var sql = new QueryChain(null, log);
							sql = req.body.insert
								? sql.insert(req.body.insert)
								: req.body.update
									? sql.update(req.body.update)
									: req.body.delete
										? sql.delete(req.body.delete)
										: req.body.columns
											? sql.columns(...(Array.isArray(req.body.columns) ? req.body.columns : [req.body.columns]))
											: sql.select(req.body.select);
							sql = sql
								.table(this._object.table)
//								.select(req.body.select)
								.join(req.body.join)
								.where({ ...req.query, ...req.body.where })
								.group(...(Array.isArray(req.body.group) ? req.body.group : [req.body.group]))
								.order(req.body.order)
								.limit(req.body.limit)
								.offset(req.body.offset)
								.distinct(req.body.distinct);
							this._object.query = {
								doc: sql,
								len: () => {
//
//									console.log('body', req.body);
									return req.body.count || req.body.len
										? req.body.group
											? new QueryChain(null, log)
												.table({ query: sql.limit().offset() })
												.select({ $: 'count', count: 'COUNT(*)' })
											: new QueryChain(null, log)
												.table(this._object.table)
												.select({ $: 'count', count: 'COUNT(*)' })
												.join(req.body.join)
												.where({ ...req.query, ...req.body.where })
//												.group(...group)
											.limit(0)
										: null;
								}
							};
						} else {
							this._object.query = Array.isArray(req.body)
								? req.body.map(query => new QueryChain(null, log).query(null, query))
								: Object.keys(req.body).reduce((prev, curr) => ({ ...prev, [curr]: new QueryChain(null, log).query(null, req.body[curr]) }), {});
						}
					}
				} else {
					let query = Object.keys(req.query).reduce((prev, curr) => {
						prev[curr] = req.query[curr] && req.query[curr].includes('|') && req.query[curr].split('|') || req.query[curr];
						return prev;
					}, {});

					if (/*!isNaN(id)*/ids.length)										// SELECT BY ID
						this.where({ ...query, [key]: /*id*/ids }).limit(0);
					else if (param)														// SELECT COLUMNS
						this.select(...param.split('|').map(this._alias)).where(query);
					else																// SELECT ALL
						this.where(query);
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

	insert(...vals) {
		return super.insert(...this._cipherRows(vals.filter(el => el)));
	}

	update(...vals) {
		return super.update(...this._cipherRows(vals.filter(el => el)));
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
								? { ...docs, err: err ? (!!this._msg || err) : undefined, msg: err ? (this._msg || err.toString()) : undefined }
								: { doc: docs, len: lens, err: err ? (!!this._msg || err) : undefined, msg: err ? (this._msg || err.toString()) : undefined });
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
						let query = obj instanceof QueryChain ? obj : new QueryChain(null, this._log).query(obj, this._object.queryValues);
						query.fetch((err, doc, len, sql) => {
							if (query._object.query == 'BEGIN')
								tran++;
							else if (query._object.query == 'COMMIT' || query._object.query == 'ROLLBACK')
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

		if (Array.isArray(this._object.query)
			? this._object.query.some(el => typeof (el) == 'function')
			: this._object.query instanceof Object
				? Object.values(this._object.query).some(el => typeof (el) == 'function')
				: false)
			return this.batch(callback, db);

		if (this._log.obj)
			console.log('OBJ', this._object);

		let sql = this.build();

		if (this._log.sql)
			console.log('SQL', sql);

		if (sql == null || (db == null && this._db == null)) {
			if (callback)
				if (typeof(callback.end) == 'function')
					callback.end();
				else
					callback();
		} else {
			(db || this._db).query(sql, this._object.queryValues, (err, doc) => {
//				console.log(sql, err, doc);
				if (doc)
					if (Array.isArray(doc)) {
						doc = doc.map((res, i) => {
							if (res.fields && res.fields.length) {
								let queries = Array.isArray(this._object.query) ? this._object.query : this._object.query instanceof Object ? Object.values(this._object.query) : [];
								let query = queries.length > i && queries[i] && queries[i]._convertRes ? queries[i] : this;
								return query._convertRes(res);
							} else {
								return res.rowCount;
							}
						});

						if (this._object.query instanceof Object && !Array.isArray(this._object.query))
							doc = Object.keys(this._object.query).reduce((prev, curr, index) => {
								if (doc.length > index)
									prev[curr] = doc[index];
								return prev;
							}, {});
					} else if (doc.fields && doc.fields.length) {
						doc = this._convertRes(doc);
					} else {
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
						let isObject = this._object.query instanceof Object && !Array.isArray(this._object.query);

						if (typeof (callback.send) == 'function')
							callback.status(err ? 400 : 200).send({ ...(isObject ? doc : { doc }), err: err ? (!!this._msg || err) : undefined, msg: err ? (this._msg || err.toString()) : undefined });
						else if (typeof (callback) == 'function')
							try {
								if (isObject && doc && doc.doc !== undefined && doc.len !== undefined)
									callback(err, doc.doc, doc.len, sql);
								else
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

		return sql;
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
