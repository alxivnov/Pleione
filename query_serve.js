"use strict";

const QueryBatch = require('./query_batch.js');

// Processes http requests and responses.
module.exports = class QueryServe extends QueryBatch {
	_alias(el) {
		let arr = el.split('~');
		return arr.length > 1 ? { [arr[1]]: arr[0] } : el;
	}

	_excludeInj(select) {
		let injection = typeof (select) == 'string' && select.match(/INSERT\s+INTO\s+\w+\s+|DELETE\s+FROM\s+(ONLY\s+)*\w+\s+|UPDATE\s+(ONLY\s+)*\w+\s+/)
			|| Array.isArray(select) && select.filter(this._excludeInj).length < select.length
			|| typeof (select) == 'object' && Object.values(select).some(val => {
				return typeof (val) == 'string' && val.match(/INSERT\s+INTO\s+\w+\s+|DELETE\s+FROM\s+(ONLY\s+)*\w+\s+|UPDATE\s+(ONLY\s+)*\w+\s+/)
					|| typeof (val) == 'object' && val && (val.insert || val.delete || val.update)
			});
		return !injection;
	}

	_encrypt(cleartext, opt) {
		opt = Object.assign({}, this._opt.enc, opt);
		return this._cipher(cleartext, opt.password, { ...opt, decipher: false });
	}

	_decrypt(encrypted, opt) {
		opt = Object.assign({}, this._opt.enc, opt);
		return this._cipher(encrypted, opt.password, { ...opt, decipher: true });
	}

	query(query, values) {
		var clone = this._clone(true);

		if (query && typeof (query.is) == 'function' && values /*&& values.$*/) {
			let req = query;
			let key = values.$;
			let val = values._;
			let json = val && val.includes('json');

			let table = values.table || req.params.table;
			let param = values.id || values[key] || req.params.id || req.params[key];// || req.params[0];
			// console.log('key:', key, 'val:', val, 'table:', table, 'param:', param, 'values:', values, 'req.params:', req.params);
			if (val) {
				let other = Object.keys(values).filter(el => ![ '$', '_' ].includes(el)).reduce((prev, curr) => {
					prev[curr] = values[curr];

					return prev;
				}, {});

				clone = clone.table(table);

				if (req.method == 'POST') {
					if (param) {
						if (req.body != null) {
							let str = json ? JSON.stringify(req.body).replace(/'/g, `''`) : req.body;

							clone = clone.insert({ ...other, [key]: param, [val]: str })
								.conflict(...Object.keys(other), key)
								.update({ [val]: str });
						} else {
							clone = clone.delete().where({ ...other, [key]: param });
						}
					} else {
						let columns = Object.keys(req.body);

						let del = columns.filter(el => req.body[el] == null);
						let ins = columns.filter(el => req.body[el] != null);
						clone = clone.query([
//							this._clone(instance).begin(),

							function () {
								return del.length
									? clone._clone()
										.table(table || this._object.table)
										.delete()
										.where({ ...other, [key]: del })
									: null
							},
							function () {
								return ins.length
									? clone._clone()
										.table(table || this._object.table)
										.insert(...ins.map(el => {
											return { ...other, [key]: el, [val]: json ? JSON.stringify(req.body[el]).replace(/'/g, `''`) : req.body[el] };
										}))
										.conflict(...Object.keys(other), key)
										.update({ [val]: { $: `excluded.${val}` } })
									: null
							}
						]);
					}
				} else {
					if (param) {
						let settings = Array.isArray(param) ? param : param.split('|');

						if (settings.length > 1)
							clone = clone.select({ $: key, _: val }, val).where({ ...other, [key]: settings });
						else
							clone = clone.select({ $: val }).where({ ...other, [key]: param }).limit(0);
					} else {
						clone = clone.select({ $: key, _: val }, val).where(other);
					}
				}
			} else {
//				let id = parseInt(param);
				let ids = param && (Array.isArray(param) ? param : param.split('|')).map(id => parseInt(id)).filter(id => !isNaN(id)) || [];

				if (table) {
					let tables = table.split('|').map(clone._alias);
					clone = clone.table(tables);

					if (tables.length > 1) {
						let alias = typeof (tables[0]) == 'object' ? Object.keys(tables[0])[0] : table;
						key = `${alias}.${key}`;
					}
				}

				if (req.method == 'POST' && (param || req.body)) {
					if (/*!isNaN(id) && id > 0*/ids.length && ids.every(id => id > 0))		// UPDATE BY ID
						clone = clone.update(req.body).where({ [key]: /*id*/ids });
					else if (param == '*')													// UPDATE WHERE
						clone = clone.update(req.body.update).where({ ...req.query, ...(Array.isArray(req.body.where) || typeof (req.body.where) == 'string' ? { OR: req.body.where } : req.body.where) });
					else if (/*!isNaN(id) && id < 0*/ids.length && ids.every(id => id < 0))	// DELETE BY ID
						clone = clone.delete().where({ [key]: /*Math.abs(id)*/ids.map(id => Math.abs(id)) });
					else if (param == '-')													// DELETE WHERE
						clone = clone.delete().where({ ...req.query, ...(Array.isArray(req.body.where) || typeof (req.body.where) == 'string' ? { OR: req.body.where } : req.body.where) });
					else if (param)															// INSERT: +, 0
						clone = clone.insert(...(Array.isArray(req.body) ? req.body : [req.body]))/*.conflict(key)*/.update();
					else {
						let log = req.body && req.body.log
							? Object.keys(clone._opt.log).reduce((log, key) => {
								log[key] = typeof (req.body.log) == 'string'
									? req.body.log.includes(key)
									: log[key] = req.body.log[key];

								return log;
							}, clone._opt.log)
							: clone._opt.log;

						if (table) {
							var sql = this._clone({ log });
							sql = req.body.insert
								? sql.insert(req.body.insert)
								: req.body.update
									? sql.update(req.body.update)
									: req.body.delete
										? sql.delete(req.body.delete)
										: req.body.columns
											? sql.columns(...(Array.isArray(req.body.columns) ? req.body.columns : [req.body.columns]))
											: sql.select(...(Array.isArray(req.body.select) ? req.body.select : [req.body.select]).filter(this._excludeInj));
							sql = sql
								.table(clone._object.table)
//								.select(req.body.select)
								.join(...(Array.isArray(req.body.join) ? req.body.join : [req.body.join]))
								.where({ ...req.query, ...(Array.isArray(req.body.where) || typeof (req.body.where) == 'string' ? { OR: req.body.where } : req.body.where) })
								.having(req.body.having)
								.group(...(Array.isArray(req.body.group) ? req.body.group : [req.body.group]))
								.order(...(Array.isArray(req.body.order) ? req.body.order : [req.body.order]))
								.limit(req.body.first ? 0 : req.body.limit)
								.offset(req.body.offset)
								.distinct(...(Array.isArray(req.body.distinct) ? req.body.distinct : [req.body.distinct]));
							clone._object.query = {
								$doc: sql,
								$len: req.body.count || req.body.len
									? req.body.distinct || req.body.group
										? this._clone({ log })
											.query(null, {
												table: sql/*this*/._object.table,
												join: sql/*this*/._object.join,
												where: sql/*this*/._object.where,
//												group: instance._object.group
											})
											.select({
												$: 'count',
												count: `COUNT(DISTINCT (${
													(req.body.distinct || req.body.group)
														.map(col => clone._wrap(Array.isArray(req.body.select) && req.body.select.find(x => x[col]) || req.body.select && req.body.select[col] || col))
														.join(', ')
												}))`
											})
//											.table({ query: sql._clone(true).limit().offset() })
//											.select({ $: 'count', count: 'COUNT(*)' })
											.limit(0)
	//	https://www.citusdata.com/blog/2016/10/12/count-performance/
										: (req.body.count == '~' || req.body.len == '~')
											&& !sql._where()
											&& (typeof (sql._object.table) == 'string' || Array.isArray(sql._object.table) && sql._object.table.length == 1 || typeof (sql._object.table) == 'object' && Object.keys(sql._object.table).length == 1 || req.body.join)
											? this._clone({ log })
												.table('pg_class')
												.select({ $: 'count', count: 'reltuples' })
												.where({ relname: sql._table().split(/[,\s]/)[0] })
												.limit(0)
											: this._clone({ log })
												.table(clone._object.table)
												.select({ $: 'count', count: 'COUNT(*)' })
												.join(...(Array.isArray(req.body.join) ? req.body.join : [req.body.join]))
												.where({ ...req.query, ...(Array.isArray(req.body.where) || typeof (req.body.where) == 'string' ? { OR: req.body.where } : req.body.where) })
	//											.group(...group)
												.limit(0)
									: null
							};
						} else {
							clone._object.query = Array.isArray(req.body)
								? req.body.map(query => this._clone({ log }).query(null, query))
								: Object.keys(req.body).reduce((prev, curr) => ({ ...prev, [curr]: this._clone({ log }).query(null, req.body[curr]) }), {});
						}
					}
				} else {
					let query = Object.keys(req.query).reduce((prev, curr) => {
						prev[curr] = req.query[curr] && req.query[curr].includes('|') && req.query[curr].split('|') || req.query[curr];
						return prev;
					}, {});

					if (/*!isNaN(id)*/ids.length)										// SELECT BY ID
						clone = clone.where({ ...query, [key]: /*id*/ids }).limit(0);
					else if (param)														// SELECT COLUMNS
						clone = clone.select(...(Array.isArray(param) ? param : param.split('|')).filter(this._excludeInj).map(clone._alias)).where(query);
					else																// SELECT ALL
						clone = clone.where(query);
				}
			}

			if (req.headers['accept'] && req.headers['accept'].includes('application/jsont'))
				clone._opt.jsont = true;
		} else {
			clone = super.query(query, values);
		}

		return clone;
	}

	// count(callback, options, ...args) {
	// 	if (callback && typeof (callback.send) == 'function')
	// 		return super.count(...args).fetch(callback, options);

	// 	return super.count(callback, options, ...args);
	// }

	fetch(callback, options) {
		if (!callback)
			return this._then(options);

		if (callback && /*(Array.isArray(callback) ? callback : [callback]).some(callback =>*/ typeof (callback.send) == 'function'/*)*/) {
			options = this._merge(options, this._opt);

			if (options.enc && options.enc.password) {
				let self = this;
				let enc = options.enc;
				options.enc = (val, decipher) => decipher
					? '\0'.repeat(self._decrypt(val, enc).length)
					: /^\0+$/.test(val)
						? undefined
						: self._encrypt(val, enc);
			}

			let isArray = Array.isArray(this._object.query);
			let isObject = this._object.query instanceof Object && !isArray;

			let fn = (err, doc, len, _sql, res) => {
				if (err === undefined && doc === undefined) {
					callback.end();
				} else if (options.jsont) {
					if (err) {
						callback.statusCode = 400;
						callback.setHeader('Content-Type', 'application/json');
						callback.end(JSON.stringify({ err, msg: err.toString() }));
					} else if (doc) {
						if (Array.isArray(res)) {
							res = res[0];
						}

						callback.setHeader('Content-Type', 'application/json');
						callback.setHeader('X-Count', doc.length);
						if (len != undefined)
							callback.setHeader('X-Total', len);
						callback.write('[\n');
						callback.write(JSON.stringify(res.fields.map(field => field.name)));
						doc.forEach((row, i) => {
							callback.write(',\n' + JSON.stringify(row));
						});
						callback.write('\n]');
						callback.end();
					}
				} else {
					callback
						.status(err ? 400 : 200)
						.send({
							...(len === undefined
								? isObject
									? doc
									: { doc }
								// $doc, $len
								: { doc, len }),

							err: err
								? (!!options.msg || err)
								: undefined,
							msg: err
								? (!!options.msg || err.toString())
								: undefined
						});
				}
			};

			return super.fetch(fn, options);
		}

		return super.fetch(callback, options);
	}

	serve(req, res) {
		let settings = req.params.table && (req.params.table === 'settings' || req.params.table.endsWith('_settings'));
		if (settings && req.method == 'POST') {
			if (req.params.id && !req.params.id.includes('|')) {
				if (req.params.id.toLowerCase().endsWith('__enc'))
					req.body = this._encrypt(req.body);
			} else {
				Object.keys(req.body).forEach(key => {
					if (key.toLowerCase().endsWith('__enc')) {
						if (/^\0+$/.test(req.body[key]))
							delete req.body[key];
						else
							req.body[key] = this._encrypt(req.body[key]);
					}
				});
			}
		}
		return this
			.query(req, settings
				? { $: 'setting', _: '_json', package: process.env.npm_package_name, ...req.query }
				: { $: '_id' }
			)
			.fetch(settings && req.method === 'GET'
				? (err, doc) => {
					if (err) {
						res.status(500).end();
					} else {
						if (req.params.id && !req.params.id.includes('|')) {
							if (req.params.id.toLowerCase().endsWith('__enc')) {
								let cleartext = this._decrypt(doc);

								doc = cleartext ? '\0'.repeat(cleartext.length) : cleartext;
							}
						} else {
							Object.keys(doc).forEach(key => {
								if (key.toLowerCase().endsWith('__enc')) {
									let cleartext = this._decrypt(doc[key]);

									doc[key] = cleartext ? '\0'.repeat(cleartext.length) : cleartext;
								}
							});
						}

						res.json(doc);
					}
				}
				: res
			);
	}
};
