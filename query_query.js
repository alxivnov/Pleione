"use strict";

const crypto = require('crypto');
const fs = require('fs');

const QueryBuild = require('./query_build.js');

// Queries DB with chained commands.
// Processes result according to S_.
// Encrypts/decrypts results.
module.exports = class QueryQuery extends QueryBuild {
	constructor(opt, obj) {
		var log = {
			doc: false,
			err: false,
			obj: false,
			sql: false
		};
		if (opt) {
			if (typeof (opt.log) == 'string' || Array.isArray(opt.log))
				opt.log = {
					doc: opt.log.includes('doc'),
					err: opt.log.includes('err'),
					obj: opt.log.includes('obj'),
					sql: opt.log.includes('sql')
				};
			else if (typeof (opt.log) == 'object')
				opt.log = { ...log, ...opt.log };
			else
				opt.log = log;
		} else {
			opt = { log };
		}

		super(opt, obj);
	}

	_merge(...args) {
		let obj = { ...args[0] };
		args.slice(1).forEach(arg => Object.keys(arg).filter(key => obj[key] == null && arg[key] != null).forEach(key => obj[key] = arg[key]));
		return obj;
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

	_isCipherColumn(col, aliases) {
		if (col.toLowerCase().endsWith('__enc'))
			return true;
		else if (Array.isArray(aliases) && aliases.length)
			return aliases
				.filter(alias => typeof (alias) == 'object')
				.some(alias => {
					return Array.isArray(alias)
						? this._isCipherColumn(col, alias)
						: typeof (alias[col]) == 'string' && alias[col].toLowerCase().endsWith('__enc');
				});
		else
			return false;
	}

	_cipherRows(res, options, decipher) {
		let rows = res.rows || res;

		options = this._merge(options, this._opt);

		let cipherFunc = typeof (options.enc) == 'function';

		let fields = options.jsont && res.fields.map((field, index) => ({ name: field.name, index }));
		let temp = (cipherFunc || options.enc && options.enc.password) && rows.map(row => fields ? [...row] : { ...row }).map(row => {
			(fields || Object.keys(row))
				.filter(key => this._isCipherColumn(key.name || key, this._object.select))
				.map(key => key.index || key)
				.filter(key => typeof (row[key]) == 'string')
				.forEach(key => {
					if (cipherFunc) {
						let val = options.enc(row[key], decipher);
						if (val === undefined)
							delete row[key];
						else
							row[key] = val;
					} else {
						row[key] = this._cipher(row[key], options.enc.password, { ...options.enc, decipher });
					}
				});

			return row;
		});
		return temp || rows;
	}

	_columnName(column) {
		if (typeof (column) == 'object')
			return 'case';

		if (typeof (column) != 'string')
			return '?column?';

		let trimmed = column.trim();
		let start = trimmed.indexOf('(');
		let end = trimmed.indexOf(')', start);
		return start > 0
			? end == trimmed.length - 1
				? trimmed.substring(0, start).toLowerCase()
				: '?column?'
			: !trimmed.match(/\W/)
				? trimmed
				: trimmed.includes('.') || trimmed.startsWith('"') && trimmed.endsWith('"')
					? trimmed.split('.').slice(-1)[0].replace(/^"|"$/g, '')
					: '?column?';
	}

	_convertRes(res, options) {
		options = this._merge(options, this._opt);

		var doc = undefined;

		let fields = options.jsont && res.fields.reduce((prev, { name }, i) => ({ [name]: String(i), ...prev }), {});
		let cols = this._object.select || this._object.return || this._object.tables || this._object.columns;
		let col = (cols || []).reduce((col, el) => {
			if (el instanceof Object) {
				let types = ['object', 'string'];
				if (types.includes(typeof (el.$)))
					col.$ = fields ? fields[el.$] : this._columnName(el.$);
				if (types.includes(typeof (el._)))
					col._ = fields ? fields[el._] : this._columnName(el._);
				else if (Array.isArray(el._))
					col._ = el._
						.filter(x => types.includes(typeof (x)))
						.map(x => fields ? fields[x] : this._columnName(x));
			}

			return col;
		}, {});
		var rows = this._cipherRows(res, options, true);
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

			doc = options.first == 1 ? res.rowCount > 0 ? rows[0] : null : rows;
		}

		return doc;
	}

	query(query, values) {
		var clone = this._clone(true);

		if (/*query*/typeof (query) == 'string' && query.endsWith('.sql')) {
			clone._object.query = fs.readFileSync(query, 'utf8').trim();
			clone._object.queryValues = values;
		} else {
			if (query && (Array.isArray(query) || typeof (query) == 'object')) {
				if (Array.isArray(query) && query.every(el => typeof (el) == 'string' && !el.includes(' '))) {

				} else {
					clone._object.query = query;//'BEGIN;\n' + query.map(obj => (obj instanceof QueryBuild ? obj : this._clone().query(obj, values)).build()).join('; ') + ';\nCOMMIT';
					clone._opt = this._merge(values, clone._opt);

					return clone;
				}
			}

			clone = super.query(query, values);
		}

		return clone;
	}

	_insertValues(keys, values) {
		values = this._cipherRows((values || this._object.insert).filter(el => el), {}, false)

		return super._insertValues(keys, values);
	}
	_update(update) {
		update = this._cipherRows((update || this._object.update).filter(el => el), {}, false);

		return super._update(update);
	}

	_callDoc(method, ...args) {
		if (this._object.query && this._object.query.doc instanceof QueryQuery)
			return this._object.query.doc[method](...args);
	}

	_callDocOrSuper(method, ...args) {
		return this._callDoc(method, ...args) || super[method](...args);
	}

	// insert(...vals) {
	// 	return this._callDoc('insert', ...vals) || super.insert(...this._cipherRows(vals.filter(el => el)));
	// }

	// update(...vals) {
	// 	return this._callDoc('update', ...vals) || super.update(...this._cipherRows(vals.filter(el => el)));
	// }

	table() {
		return this._callDocOrSuper('table', ...arguments);
	}
	join() {
		return this._callDocOrSuper('join', ...arguments);
	}
	exists() {
		return this._callDocOrSuper('exists', ...arguments);
	}
	select() {
		return this._callDocOrSuper('select', ...arguments);
	}
	return() {
		return this._callDocOrSuper('return', ...arguments);
	}
	insert() {
		return this._callDocOrSuper('insert', ...arguments);
	}
	update() {
		return this._callDocOrSuper('update', ...arguments);
	}
	delete() {
		return this._callDocOrSuper('delete', ...arguments);
	}
	where() {
		return this._callDocOrSuper('where', ...arguments);
	}
	group() {
		return this._callDocOrSuper('group', ...arguments);
	}
	order() {
		return this._callDocOrSuper('order', ...arguments);
	}
	limit() {
		return this._callDocOrSuper('limit', ...arguments);
	}
	offset() {
		return this._callDocOrSuper('offset', ...arguments);
	}
	having() {
		return this._callDocOrSuper('having', ...arguments);
	}
	distinct() {
		return this._callDocOrSuper('distinct', ...arguments);
	}
	consflict() {
		return this._callDocOrSuper('consflict', ...arguments);
	}

	fetch(callback, options) {
		if (!callback)
			return this._then(options);

		options = this._merge(options, this._opt);

		let isArray = Array.isArray(this._object.query);
		let isObject = this._object.query instanceof Object && !isArray;

		if (options.log.obj)
			console.log('OBJ', this._object);

		let sql = this.build();

		if (options.log.sql)
			console.log('SQL', sql);

		let client = options.db;

		if (sql == null || sql == '' || client == null) {
			if (callback)
				// if (typeof(callback.end) == 'function')
				// 	callback.end();
				// else
					callback();
		} else {
			let finalRes = null;
			let finalDoc = null;
			let finalErr = null;

			/*return*/ client.query({ text: sql, values: this._object.queryValues, rowMode: options.jsont && 'array' }).then((res) => {
				let doc = undefined;
				if (res) {
					finalRes = res;

					if (Array.isArray(res)) {
						doc = res.map((sub, i) => {
							if (sub.fields && sub.fields.length) {
								let queries = (isArray ? this._object.query : isObject ? Object.values(this._object.query) : [])
									.map(query => typeof (query) == 'function' ? query.call(this) : query);
								let query = queries.length > i
									? queries[i]
										? queries[i]._convertRes
											? queries[i]
											: this
										: { _convertRes: () => undefined }
									: this;
								return !(sub.fields.length == 1 && sub.fields[0].name == '__null__')
									? query._convertRes(sub, options)
									: sub.rowCount;
							} else {
								return sub.rowCount;
							}
						});

						if (isObject)
							doc = Object.keys(this._object.query).reduce((prev, curr, index) => {
								if (doc.length > index)
									prev[curr] = doc[index];
								return prev;
							}, {});
					} else if (isArray || isObject) {
						let key = isArray
							? 0
							: Object.keys(this._object.query)[0];
						let query = this._object.query[key]
							? this._object.query[key]._convertRes
								? this._object.query[key]
								: this
							: { _convertRes: () => undefined };
						let value = res.fields && res.fields.length && !(res.fields.length == 1 && res.fields[0].name == '__null__')
							? query._convertRes(res)
							: res.rowCount;
						doc = isArray
							? [value]
							: { [key]: value };
					} else if (res.fields && res.fields.length && !(res.fields.length == 1 && res.fields[0].name == '__null__')) {
						doc = this._convertRes(res, options);
					} else {
						doc = res.rowCount;
					}

					finalDoc = doc;
				}
			}).catch((err) => {
				if (err) {
					if (err.position && err.position < sql.length) {
						// let pos = parseInt(err.position);
						let start = sql.lastIndexOf('\n', err.position);
						let end = sql.indexOf('\n', err.position);

						var line = 0;
						var pos = undefined;
						while (pos === undefined || pos > -1 && pos < err.position) {
							pos = sql.indexOf('\n', pos === undefined ? pos : pos + 1);
							line++;
						}
						err.message += ` [line ${line}: ${sql.substring(start > -0 ? start : 0, end > -1 ? end : sql.length).trim()}]`;
					} else if (err.where) {
						err.message += ` [where: ${err.where.length > 128 ? err.where.substring(0, 128) + '...' : err.where}]`;
					} else if (err.hint) {
						err.message += ` [hint: ${err.hint}]`;
					} else {
						err.message += ` [query: ${sql.length > 128 ? sql.substring(0, 128) + '...' : sql}]`;
					}

					if (options.log.err) {
						// const MAX_SQL_LENGTH = 512;

						// if (!options.log.sql)
						// 	console.log('SQL', options.sql && sql.length > options.sql ? sql.substring(0, options.sql) + '...' : sql);

						console.error('ERR', err.toString()/*, `[${err.hint}]`*/);
					}

					finalErr = err;
				}
			}).finally(() => {
				if (options.log.doc) {
					console.log('DOC', doc);
				}

				// console.log('query', finalRes, finalDoc, finalErr);

				if (callback)
					/*(Array.isArray(callback) ? callback : [callback]).forEach(callback =>*/ {
						/*if (typeof (callback.send) == 'function')
							callback.status(err ? 400 : 200).send({ ...(isObject ? doc : { doc }), err: err ? (!!options.msg || err) : undefined, msg: err ? (options.msg || err.toString()) : undefined });
						else*/ if (typeof (callback) == 'function')
							try {
								if (isObject && finalDoc && finalDoc.$doc !== undefined && finalDoc.$len !== undefined)
									callback(finalErr, finalDoc.$doc, finalDoc.$len, sql, finalRes);
								else
									callback(finalErr, finalDoc, undefined, sql, finalRes);
							} catch (ex) {
								console.error('query', ex);

								if (options.err && typeof(options.err) == 'function')
									options.err(ex);
								else
									throw ex;
							}
					}/*);*/
			});
		}

		return sql;
	}

	/*
	WARNING: Switch execute to fetch
	*/
	_then(onfulfilled, onrejected) {
		let thenable = typeof (onfulfilled) == 'functiom';
		let options = typeof (onfulfilled) == 'object' ? onfulfilled : undefined;
		let promise = new Promise((resolve, reject) => {
			this.fetch((err, doc) => {
				if (err)
					reject(err);
				else
					resolve(doc);
			}, options);
		});
		return thenable ? promise.then(onfulfilled, onrejected) : promise;
	}

	print(options) {
		options = this._merge(options, this._opt);

		if (options.db)
			this.fetch((err, doc, len, sql) => {
				console.log({ doc: doc, err: err, len: len, sql: sql });
			}, options);
		else
			super.print();
	}
}
