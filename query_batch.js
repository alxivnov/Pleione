"use strict";

const QueryQuery = require('./query_query.js');

// Executes SQL queries in batches.
// Executes transactions.
// Executes count and first queries.
module.exports = class QueryBatch extends QueryQuery {
	_count(count) {
		return (count || this._opt.count) == '*'
			? 'COUNT(*)'
			: `COUNT(${this._wrap(count || this._opt.count)})`;
	}

	limit(limit) {
		let query = super.limit(limit === 0 ? 1 : limit);
		if (limit === 0)
			query._opt.first = 1;
		return query;
	}

	fetch(callback, options) {
		if (!callback)
			return this._then(options);

		options = this._merge(options, this._opt);

		if (options.count) {
			// https://www.postgresql.org/docs/current/tutorial-agg.html
			// https://www.postgresql.org/docs/current/functions-aggregate.html
			// count, sum, avg, max, min
			// array_agg
			// bit_and, bit_or, bit_xor
			// bool_and, bool_or, every
			// json_agg, jsonb_agg, json_object_agg, jsonb_object_agg
			// range_agg, range_intersect_agg
			// string_agg
			// xmlagg
			let sub = this._object.select && this._select().startsWith(/*'('*/'DISTINCT');
			let count = sub
				? '*'
				: this._object.distinct || this._object.group
					? `DISTINCT (${this._distinct() || this._group()})`
					: options.count;
//			if (this._object.select)
				this._object.query = {
					$doc: this._clone({ ...options, count: undefined })
						.query(null, { ...this._object}),
					$len: this._clone({ ...options, count: undefined })
						.query(null, sub
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
//								group: this._object.group
							})
						.select({ $: 'count', count: this._count(count) })
						.limit(0)
				};
//			else
//				this.select({ $: 'count', count: this._count() });
		}

		let isArray = Array.isArray(this._object.query);
		let isObject = this._object.query instanceof Object && !isArray;

		let hasArray = (queries) => queries.some(el => typeof (el) == 'function' && (el.length > 0 || Array.isArray(el())) || Array.isArray(el) || el instanceof QueryQuery && (el._object.createDatabase || el._object.dropDatabase || el._object.query == 'BEGIN'));
		if ((isArray || isObject) && (options.forceQueuing || options.ignoreErrors) || (isArray
			? hasArray(this._object.query)
			: isObject
				? hasArray(Object.values(this._object.query))
				: false))
			return this.batch(callback, options);

		return super.fetch(callback, options);
	}

	_assignKeys(docs) {
		return Object.assign(docs, Object.keys(this._object.query).reduce((prev, curr, index) => {
			if (docs.length > index)
				prev[curr] = docs[index];
			return prev;
		}, {}));
	}

	batch(callback, options) {
		options = this._merge(options, this._opt);

		let client = options.db;
		let isArray = Array.isArray(this._object.query);
		let isObject = this._object.query instanceof Object && !isArray;
		let connect = (err, client, done) => {
			var tran = 0;

			const exit = (errs, docs, lens, sqls, secs, ress) => {
				// if (typeof (client.end) == 'function')
				// 	client.end();

				if (done)
					done();

				var err = null;
				if (Array.isArray(errs)) {
					if (errs.some(err => err)) {
						err = new Error('Batch error');// AggregateError
						err.errors = errs;
					} else {
						err = null;
					}
				} else {
					err = errs;
				}

				if (callback)
					/*(Array.isArray(callback) ? callback : [callback]).forEach(callback =>*/ {
						if (isObject)
							docs = this._assignKeys(docs);

						/*if (typeof (callback.send) == 'function')
							callback.send(isObject
								? { ...docs, err: err ? (!!options.msg || err) : undefined, msg: err ? (options.msg || err.toString()) : undefined }
								: { doc: docs, len: lens, err: err ? (!!options.msg || err) : undefined, msg: err ? (options.msg || err.toString()) : undefined });
						else*/ if (isObject && docs.$doc !== undefined && docs.$len !== undefined)
							callback(err, docs.$doc, docs.$len, sqls, secs, ress);
						else
							callback(err, docs, lens, sqls, secs, ress);
					}/*);*/
			};

			const next = (errs, docs, lens, sqls, secs, ress, idx) => {
				let arr = isArray ? this._object.query : /*isObject ?*/ Object.values(this._object.query) /*: []*/;

				if (errs && !Array.isArray(errs)) {
					if (tran)
						this._clone(options).rollback().fetch(() => {
							exit(errs, docs, lens, sqls, secs, ress);
						}, { ...options, count: undefined, db: client });
					else
						exit(errs, docs, lens, sqls, secs, ress);
				} else if (idx < arr.length) {
					let obj = arr[idx];

					// console.log('batch next', idx, isObject ? Object.keys(this._object.query)[idx] : undefined);
					let promise = new Promise((resolve, reject) => {
						try {
							resolve(typeof (obj) == 'function' ? obj.call(this, this._assignKeys([...docs]), /*lens, */errs, sqls) : obj);
						} catch (err) {
							reject(err);
						}
					});
					// let promise = Promise.resolve(typeof (obj) == 'function' ? obj.call(this, docs, /*lens, */errs, sqls) : obj);
					promise.then(obj => {
						if (obj) {
							let query = obj instanceof QueryQuery ? obj : this._clone(options).query(obj, this._object.queryValues);
							// console.log('fetch', typeof (obj), Array.isArray(obj) && obj.length, obj instanceof QueryQuery && obj._opt.db._connectionString, options.db._connectionString);
							query.fetch((err, doc, len, sql, sec, res) => {
								// console.log('batch fetch', idx, isObject ? Object.keys(this._object.query)[idx] : undefined, err);
								if (query._object.query == 'BEGIN')
									tran++;
								else if (query._object.query == 'COMMIT' || query._object.query == 'ROLLBACK')
									tran--;

								if (Array.isArray(errs))
									errs.push(err);
								else
									errs = err;

								docs.push(doc);
								lens.push(len);
								sqls.push(sql);
								secs.push(sec);
								ress.push(res);

								next(errs, docs, lens, sqls, secs, ress, idx + 1);
							}, { ...options, count: undefined, db: client });
						} else {
							if (Array.isArray(errs))
								errs.push(null);
							else
								errs = null;

							docs.push(null);
							lens.push(null);
							sqls.push(null);
							secs.push(null);
							ress.push(null);

							next(errs, docs, lens, sqls, secs, ress, idx + 1);
						}
					}).catch(err => {
						if (Array.isArray(errs))
							errs.push(err);
						else
							errs = err;

						docs.push(null);
						lens.push(null);
						sqls.push(null);
						secs.push(null);
						ress.push(null);
						next(errs, docs, lens, sqls, secs, ress, idx + 1);
					});
				} else {
					if (tran)
						this._clone(options).commit().fetch(err => {
							if (Array.isArray(errs))
								errs.push(err);
							else
								errs = err;

							exit(errs, docs, lens, sqls, secs, ress);
						}, { ...options, count: undefined, db: client });
					else
						exit(errs, docs, lens, sqls, secs, ress);
				}
			};

//			this._clone(options).begin().fetch(err => {
				next(options.ignoreErrors ? [ ] : null/*err*/, [ ], [ ], [ ], [ ], [ ], 0);
//			}, , { db: client });
		};
		let queries = isArray ? this._object.query : isObject ? Object.values(this._object.query) : [];
		let isBegin = (sql) => typeof (sql) == 'string' && (sql == 'BEGIN' || sql.includes('BEGIN;'));
		let tx = queries.some(el => el instanceof QueryQuery && (el._object.createDatabase || el._object.dropDatabase || isBegin(el._object.query))
			|| isBegin(el));
		if (tx)
			client.connect(connect);
		else
			connect(null, client, () => { });
	}

	// begin(queries) {
	// 	if (Array.isArray(queries) || typeof (queries) == 'object') {
	// 		let $begin = this._clone().begin();
	// 		return this.query(Array.isArray(queries) ? [ $begin, ...queries ] : { sbegin, ...queries });
	// 	}

	// 	return super.begin();
	// }

	// count with join?
	count(callback, options) {
		return this.fetch(callback, { ...options, count: '*' });
	}

	/*
	WARNING: Switch first to fetching
	*/
	first(callback, options) {
		return this.limit(1).fetch(callback, { ...options, /*count: undefined,*/ first: 1 });
	}
}
