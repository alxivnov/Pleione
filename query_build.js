(function (e, t) {
	typeof exports == 'object' && typeof module != 'undefined'
		? module.exports = t()
		: typeof define == 'function' && define.amd
			? define(t)
			: e.QueryBuild = t(e)
})(this, function (e) {
	"use strict";

	const QueryChain = e && e.QueryChain || require('./query_chain.js');

	const RESERVED_KEYS = new Set(['$', '_']);
	// https://www.postgresql.org/docs/current/functions-comparison.html
	const COMPARISON = new Set(['>', '<', '>=', '<=', '=', '<>', '!=']);
	// https://www.postgresql.org/docs/current/functions-matching.html
	const MATCHING = new Set(['LIKE', 'ILIKE']);

	// Build an SQL query out of chained commands.
	return class QueryBuild extends QueryChain {
		static RESERVED_KEYS = RESERVED_KEYS;

		_wrap(col, braces) {
			return col.match(/\W/) ? braces && col.match(/\s/) ? `(${col})` : col : `"${col}"`;
		}

		_case(obj) {
			return typeof (obj) == 'object' ? Object.keys(obj).reduce((prev, curr) => {
				return prev + ` WHEN ${this._where(obj[curr])} THEN '${curr}'`
			}, 'CASE') + ' ELSE NULL END' : obj;
		}

		_table() {
			return Array.isArray(this._object.table)
				? this._object.table.map(el => {
					return el instanceof Object
						? el.select
							? `(${this._clone().query(null, el).build()}) query`
							: Object.keys(el)
								.filter(key => !(this._object.join && this._object.join.some(join => Object.keys(join).includes(key))))
								.map(key => `${el[key]} AS ${key}`)
						: !(this._object.join && this._object.join.some(join => Object.keys(join).includes(el)))
							? el
							: undefined
				}).filter(el => el && !(Array.isArray(el) && el.length == 0) && !(typeof (el) == 'object' && Object.keys(el).length == 0)).join(', ')
				: this._object.table instanceof Object
					? this._object.table.select
						? `(${this._clone().query(null, this._object.table).build()}) query`
						: Object.keys(this._object.table)
							.filter(key => !(this._object.join && this._object.join.some(join => Object.keys(join).includes(key))))
							.map(key => `${this._object.table[key] instanceof QueryBuild ? '(' + this._object.table[key] + ')' : this._object.table[key]} AS ${key}`)
							.join(', ')
					: this._object.table;
		}

		_join(join_type) {
			let arr = this._object.join;

			if (!arr)
				return null;

			let aliases = Array.isArray(this._object.table) ? this._object.table : [ this._object.table ];
			return `${arr.map((obj) => {
				return obj instanceof Object
					? Object.keys(obj).map(key => {
						let val = obj[key];
						if (typeof (val) == 'object' && val.from)
							return {
								join: val.join || join_type,
								from: `(${this._clone().query(null, val.from).build()})`,
								as: key,
								on: val.on
							}
						else if (typeof (val) == 'string')
							return {
								join: join_type,
								from: val,
								as: key
							};

						let alias = aliases.find(alias => alias[key]);
						return {
							join: join_type,
							from: alias ? alias[key] : key,
							as: alias ? key : undefined,
							on: val
						};
					}).map((obj) => {
						let join = obj.join ? `${obj.join} ` : '';
						let from = `JOIN ${obj.from}`;
						let as = obj.as ? ` AS ${obj.as}` : '';
						let on = obj.on ? ` ON ${this._where(obj.on)}` : '';
						return join + from + as + on;
					}).join(' ')
					: obj;
			}).join(` ${join_type} `)}`;
		}

		_select() {
			let cols = this._object.select;

			if (cols.length == 0)
				return '*';
			else
				return cols.map(obj => {
					return Array.isArray(obj)
						? obj
							.filter(val => val !== undefined)
							.map(val => {
								if (val instanceof QueryBuild)
									val = `(${val.build()})`;
	//							else if (Array.isArray(val))
	//								val = `COALESCE(${val.join(', ')})`;
								else if (val instanceof Object)
									val = val.table
										? `(${this._clone().query(undefined, val).table(val.table).build()})`
										: Object.keys(val).map(key => `${val[key] instanceof Object && (val[key].$ || val[key]._) || val[key]} AS "${key}"`).join(', ');
								else if (typeof (val) == 'string' && !val.match(/\W/))
									val = `"${val}"`;

								return `${val}`;
							})
							// .filter(val => !val.match(/INSERT\s+INTO\s+\w+\s+|DELETE\s+FROM\s+(ONLY\s+)*\w+\s+|UPDATE\s+(ONLY\s+)*\w+\s+/))
							.join(', ')
						: obj instanceof Object
							? Object.keys(obj)
								.filter(key => /*key != '_' &&*/ !(RESERVED_KEYS.has(key) && obj[obj[key]] !== undefined))
								.filter(key => obj[key] !== undefined)
								.map(key => {
									let val = obj[key];

//							if (key == '$' && obj[val] !== undefined)
//								return undefined;

									if (val instanceof QueryBuild)
										val = `(${val.build()})`;
									else if (Array.isArray(val))
										val = RESERVED_KEYS.has(key)
											? val.join(', ')
											: `COALESCE(${val.join(', ')})`;
									else if (val instanceof Object)
										val = /*Object.keys(val).every(key => key == '$')
										? val.$
										:*/ val.table
												? `(${this._clone().query(undefined, val).table(val.table).build()})`
												: (val.$ || val._) || this._case(val);//Object.keys(val).map(key => `${val[key]} AS ${key}`).join(', ');
									else if (typeof (val) == 'string' && !val.match(/\W/))
										val = `"${val}"`;

									return RESERVED_KEYS.has(key) ? val : `${val} AS "${key}"`;
								})
								.join(', ')
							: typeof (obj) == 'string' && !obj.match(/\W/) ? `"${obj}"` : obj;
				})
					// .filter(val => !val.match(/INSERT\s+INTO\s+\w+\s+|DELETE\s+FROM\s+(ONLY\s+)*\w+\s+|UPDATE\s+(ONLY\s+)*\w+\s+/))
					.join(', ');
		}

		_return() {
			let cols = this._object.return;

			if (cols.length == 0)
				return "*";
			else
				return cols.map(obj => {
					return Array.isArray(obj)
						? obj
							.filter(val => val !== undefined)
							.map(val => {
								if (typeof (val) == 'string' && !val.match(/\W/))
									val = `"${val}"`;
								else if (val instanceof Object)
									val = val.$ || val._ || val;

								return `${val}`;
							}).join(', ')
						: obj instanceof Object
							? Object.keys(obj)
								.filter(key => /*key != '_' &&*/ !(RESERVED_KEYS.has(key) && obj[obj[key]] !== undefined))
								.filter(key => obj[key] !== undefined)
								.map(key => {
									let val = obj[key];

									if (typeof (val) == 'string' && !val.match(/\W/))
										val = `"${val}"`;
									else if (val instanceof Object)
										val = val.$ || val._ || val;

									return RESERVED_KEYS.has(key) ? val : `${val} AS "${key}"`;
								})
								.join(', ')
							: typeof (obj) == 'string' && !obj.match(/\W/) ? `"${obj}"` : obj;
				}).join(', ');
		}

		_insertColumns() {
			let rows = this._object.insert;

			let keys = [];
			rows.forEach(obj => {
				if (obj instanceof Object)
					Object.keys(obj).forEach(key => {
						if (keys.indexOf(key) < 0)
							keys.push(key);
					});
			});

			return keys;
		}

		_insertValues(keys, values) {
			let rows = values || this._object.insert;

			let vals = rows.map(obj => {
				return obj instanceof Object ? keys.map(key => {
					let val = obj[key];

					if (val === undefined)
						val = 'DEFAULT';
					else if (val === null)
						val = 'NULL';
					else if (typeof (val) == 'string' && val != 'NOW()' && !val.match(/^\$\d+\./))
						val = `'${val.replace(/\'/g, '\'\'').replace(/\0/g, '')}'`;
					else if (val instanceof Object && Object.keys(val).includes('$'))
						val = val.$;
					else if (val instanceof QueryBuild)
						val = `(${val.build()})`;
					else if (val instanceof Object && (val.select || val.return || val.tables || val.columns || val.table || val.query))
						val = `(${this._clone().query(null, val).build()})`;
					else if (val instanceof Object)
						val = `'${JSON.stringify(val).replace(/'/g, `''`)}'`;

					return val;
				}).join(', ') : obj;
			});

			return vals;
		}

		_insert(columns, values) {
			let keys = columns || this._insertColumns();
			let vals = values || this._insertValues(keys);
			return keys.length > 0
				? `(${keys.map(key => `"${key}"`).join(', ')}) VALUES (${vals.join('), (')})`
				: vals.join(', ');
		}

		_update(update) {
			let vals = update || this._object.update;

			return vals.map(obj => {
				return obj instanceof Object ? Object.keys(obj).map(key => {
					let col = this._wrap(key, '()');
					let val = obj[key];

					if (val === undefined)
						val = 'DEFAULT'
					else if (val === null)
						val = 'NULL'
					else if (typeof (val) == 'string' && val != 'NOW()' && !val.toLowerCase().startsWith('excluded.') && !val.match(/^\$\d+\./))
						val = `'${val.replace(/\'/g, '\'\'').replace(/\0/g, '')}'`;
					else if (val instanceof Object && Object.keys(val).includes('$'))
						val = val.$;
					else if (val instanceof QueryBuild)
						val = `(${val.build()})`;
					else if (val instanceof Object && (val.select || val.return || val.tables || val.columns || val.table || val.query))
						val = `(${this._clone().query(null, val).build()})`;
					else if (val instanceof Object)
						val = `'${JSON.stringify(val).replace(/'/g, `''`)}'`;

					return `${col}=${val}`;
				}).join(', ') : obj;
			}).join(', ');
		}

		// Comparison Operators:
		// { NOT: null, true, 1, 'some', [], query }
		// { IS: null, true, false }
		//* <										{ col: { '<': ... } }
		//* >										{ col: { '>': ... } }
		//* <=										{ col: { '<=': ... } }
		//* >=										{ col: { '>=': ... } }
		//+ =										{ col: { '=': ... } }, { col: ... }
		//+ <>										{ col: { '!=': ... } }, { col: { '!': ... } }
		//+ !=										{ col: { '<>': ... } }, { col: { '!': { '=': ... } } }
		//+ NOT										{ col: { '!': ... } }, { col: { NOT: ... } }
		//+ AND										{ ... }, { '&': ... }, { AND: ... }
		//+ OR										[ ... ], { '|': ... }, { OR: ... }
		// Comparison Predicates:
		// [NOT] BETWEEN [SYMMETRIC] ... AND ...
		// IS [NOT] DISTINCT FROM ...
		//+ IS [NOT] NULL, ISNULL, NOTNULL			{ col: null }, { col: { NOT: null } }
		//+ IS [NOT] (TRUE|FALSE)					{ col: true }, { col: { NOT: true } }
		// IS [NOT] UNKNOWN
		// Pattern Matching:
		//* LIKE									{ col: { LIKE: ... } }, { col: { '~~': ... } }
		//* ILIKE									{ col: { ILIKE: ... } }, { col: { '~~*': ... } }
		// Subquery Expressions:
		// *EXISTS									{ EXISTS: query }
		// [NOT] IN									{ col: [] }, { col: query }, { col: { NOT: query } }
		// *[=|<|...] ANY/SOME						{ col: { '>': [] } }, { col: { '>': query } }, { col: { ANY: { '>': [] } } }
		// *[=|<|...] ALL							{ col: { '>': [] } }, { col: { '>': query } }, { col: { ALL: { '>': query } } }
		_where(where, separator) {
			if (where === undefined)
				where = this._object.where;

			if (!where)
				return null;

			let args = (Array.isArray(where) ? where : [where]).map(arg => {
				if (Array.isArray(arg))
					return arg.length ? `(${this._where(arg)})` : null;
				else if (arg instanceof Object)
					return Object.keys(arg).filter(key => arg[key] !== undefined).map(key => {
						let col = this._wrap(key, '()');
						var val = arg[key];

						if (key == '$') {
							return `(${this._where(val)})`;
						} else if (key == 'AND') {
							return `(${this._where(val, 'AND')})`;
						} else if (key == 'OR') {
							return `(${this._where(val, 'OR')})`;
						} else if (key == 'NOT') {
							return `NOT (${this._where(val)})`;
						} else if (key == 'EXISTS') {
							return `EXISTS (${val})`;
						} else if (val instanceof Date) {
							val = val.toISOString();
						} else if (Array.isArray(val)) {
							let arr = val.map(tmp => typeof (tmp) == 'string'/* && tmp != 'NOW()' && !tmp.match(/^\$\d+\./)*/
								? `'${tmp.replace(/\'/g, '\'\'').replace(/\0/g, '')}'`
								: tmp instanceof Object && Object.keys(tmp).includes('$')
									? tmp.$
									: tmp);

							let isNull = arr.includes(null) ? ` OR ${col} IS NULL` : '';
							return arr.length > 0 ? `(${col} IN (${arr.filter(el => el !== null).join(', ')})${isNull})` : 'FALSE';
						} else if (val instanceof Object) {
							let keys = Object.keys(val);

							if (keys.includes('$'))
								return `${col} = (${this._where(val)})`;
							else if (keys.includes('_'))
								return `(${this._where(val._)})`;
							else if (keys.includes('AND'))
								return `(${this._where(val.AND, 'AND')})`;
							else if (keys.includes('OR'))
								return `(${this._where(val.OR, 'OR')})`;
							else if (keys.includes('NOT'))
								return val.NOT === null ? `${col} IS NOT NULL` : `NOT (${this._where(val.NOT)})`;
							else if (keys.some(key => COMPARISON.has(key)))
								return keys.map(key => {
									let sub = val[key];

									if (sub instanceof Date) {
										sub = `'${sub.toISOString()}'`;
									} else if (sub instanceof Object) {
										let exp = sub.ANY ? 'ANY' : sub.ALL ? 'ALL' : undefined;
										let query = exp ? sub[exp] : sub;
										let tmp = query instanceof QueryBuild ? query : this._clone().query(null, query);
										sub = exp ? `${exp} (${tmp})` : `(${tmp})`;
									// } else if (typeof (sub) == 'string' && val != 'NOW()' && !sub.match(/^\$\d+\./)) {
									// 	sub = `'${sub.replace(/\'/g, '\'\'').replace(/\0/g, '')}'`;
									}

									return `${col} ${key} ${sub}`;
								}).join(' AND ');
							else if (keys.some(key => MATCHING.has(key)))
								return keys.map(key => `${col} ${key} '${val[key]}'`).join(' AND ');

							let tmp = val instanceof QueryBuild ? val : this._clone().query(null, val);

							return `${col} IN (${tmp})`;
							return `${col} ${val instanceof QueryBuild || val.limit != 1 || val.first != 1 || !val.exists || tmp ? 'IN' : '='} (${tmp})`;
						}

					/*if (val === null)
						val = 'NULL';
					else */if (typeof (val) == 'string'/* && val != 'NOW()' && !val.match(/^\$\d+\./)*/)
							val = `'${val.replace(/\'/g, '\'\'').replace(/\0/g, '')}'`;
						else if (val instanceof Object && Object.keys(val).includes('$'))
							val = `(${this._where(val)})`;
						else if (val instanceof QueryBuild)
							val = `(${val.build()})`;

						return val === null ? `${col} IS NULL` : `${col} ${val instanceof QueryBuild ? 'IN' : '='} ${val}`;
					}).join(` ${where == arg && separator || 'AND'} `);
				else
					return arg;
			});
			//		let args = proc(where);

			return args.join(separator ? ` ${separator} ` : ' OR ');
		}

		_group() {
			let group = this._object.group;
			if (!group)
				return null;

			return group.map(this._wrap).join(', ');
		}

		_having() {
			let having = this._object.having;

			if (!having)
				return null;

			return this._where(this._object.having);
		}

		_distinct() {
			let distinct = this._object.distinct;
			if (!distinct)
				return null;

			return distinct.map(this._wrap).join(', ');
		}

		_order() {
			let order = this._object.order;
			if (!order)
				return null;

			let SORT_DIRECTION_TYPES = new Set(['number', 'boolean']);
			let args = order.map(arg => {
				if (arg instanceof Object)
					return Object.keys(arg).map(key => {
						var val = arg[key];

						if (typeof (key) == 'string' && !key.match(/\W/))
							key = `"${key}"`;

						if (SORT_DIRECTION_TYPES.has(typeof (val)))
							val = val > 0 ? 'ASC' : 'DESC';

						return `${key} ${val}`;
					}).join(', ');
				else
					return arg;
			});

			return args.join(', ');
		}

		_conflict() {
			let conflict = this._object.conflict;
			if (!conflict)
				return null;

			let columns = conflict.filter(el => typeof (el) == 'string' && !el.match(/\s/)).join(', ');
			let where = conflict.filter(el => typeof (el) != 'string' || el.match(/\s/)).map(el => this._where(el)).join(' AND ');

			return `ON CONFLICT` + (columns ? ` (${columns})` : '') + (where ? ` WHERE ${where}` : '');
		}

		_tables() {
			let cols = this._object.tables;

			return cols.length
				? cols.map(obj => {
					return obj instanceof Object
						? Object.keys(obj).map(key => {
							let val = obj[key];

							if (typeof (val) == 'string' && !val.match(/\W/))
								val = `"${val}"`;

							return key == '$' ? val : `${val} AS ${key}`;
						}).join(', ')
						: typeof (obj) == 'string' && !obj.match(/\W/)
							? `"${obj}"`
							: obj;
				}).join(', ')
				: '*';
		}

		_columns() {
			let cols = this._object.columns;

			return cols.length
				? cols.map(obj => {
					return obj instanceof Object
						? Object.keys(obj).map(key => {
							let val = obj[key];

							if (typeof (val) == 'string' && !val.match(/\W/))
								val = `"${val}"`;
							else if (val instanceof Object)
								val = val.$ || val._ || val;

							return key == '$' ? val : `${val} AS ${key}`;
						}).join(', ')
						: typeof (obj) == 'string' && !obj.match(/\W/)
							? `"${obj}"`
							: obj;
				}).join(', ')
				: '*';
		}

		build() {
			if (this._object.query != null) {
				let flatten = (queries) => queries
						.map(query => typeof (query) == 'function' ? query.call(this) : query)
//						.filter(query => query)
						// .flatMap(query => Array.isArray(query)
						// 	? flatten(query)
						// 	: query instanceof QueryBuild && Array.isArray(query._object.query)
						// 		? flatten(query._object.query)
						// 		: [query])
						.map(query => query && query.build ? query.build() : query)
						.map(query => query || 'SELECT NULL AS __null__ WHERE FALSE')
//						.filter(query => query)
						.join(';\n')
						+ (queries.reduce((count, query) => count + (query && query._object ? query._object.query == 'BEGIN' ? 1 : query._object.query == 'COMMIT' ? - 1 : 0 : 0), 0) ? ';\nCOMMIT;' : ';');
				return Array.isArray(this._object.query)
					? flatten(this._object.query)
					: this._object.query instanceof Object
						? flatten(Object.values(this._object.query))
						: this._object.query;
			}

			this._object.queryValues = null;

			var sql = null;

			if (this._object.insert) {
				let columns = this._insertColumns();
				let insert = this._insert(columns);
				if (!insert.length)
					return null;

				sql = `INSERT INTO ${this._table()} ${insert}`;

				if (this._object.conflict) {
					sql += ` ${this._conflict()}`;
					var update = this._object.update;
					// if update is an array of strings treat as columns to update
					// otherwise if update is empty update all columns not included in conflict
					let set = update
						? !update.length || typeof (update[0]) == 'function'
							? columns.filter(col => !this._object.conflict.includes(col))
							: update.every(el => typeof (el) == 'string' && !el.match(/\W/))
								? update
								: undefined
						: undefined;
					if (set) {
						let excluded = set
							.reduce((cols, col) => {
								if (!cols[col])
									cols[col] = { $: `excluded.${col}` };
								return cols;
							}, {});
						if (typeof (update[0]) == 'function')
							excluded = update[0](excluded);
						if (Object.keys(excluded).length)
							update = [excluded];
					}
					if (update && update.length)
						sql += ` DO UPDATE SET ${this._update(update)}`;
					let where = this._where();
					if (where)
						sql += ` WHERE ${where}`;
					if (!update || !update.length)
						sql += ' DO NOTHING';
				}

				if (this._object.return)
					sql += ` RETURNING ${this._return()}`;
				else if (this._object.select)
					sql += ` RETURNING ${this._select()}`;

				return sql;
			} else if (this._object.createDatabase)
				return `CREATE DATABASE ${this._object.createDatabase} OWNER ${this._object.owner || 'DEFAULT'}`;
			else if (this._object.dropDatabase)
				return `DROP DATABASE IF EXISTS ${this._object.dropDatabase}`;
			else if (this._object.tables) {
				let where = this._where();
				let query = `SELECT ${this._tables()} FROM pg_tables`;
				return query + (where ? ` WHERE ${where}` : '');
			} else if (this._object.columns)
				return `SELECT ${this._columns()}
					FROM information_schema.columns
					WHERE table_name = '${this._table()}'`;
			else if (this._object.addColumn)
				return `ALTER TABLE ${this._table()} ${this._object.addColumn}`;
			else if (this._object.dropColumn)
				return `ALTER TABLE ${this._table()} ${this._object.dropColumn}`;
			else if (this._object.update) {
				let update = this._update();
				if (!update.length)
					return null;

				sql = `UPDATE ${this._table()} SET ${update}`;
			} else if (this._object.delete) {
				sql = `DELETE FROM ${this._table()}`;
			} else {
				sql = 'SELECT';

				let distinct = this._distinct();
				if (distinct)
					sql += ` DISTINCT ON (${distinct})`;

				if (this._object.select)
					sql += ` ${this._select()}`
				else if (this._object.exists)
					sql += ' 1';
				else
					sql += ' *';

				if (this._object.table)
					sql += ` FROM ${this._table()}`;
			}

			if (this._object.update || this._object.delete) {
				let where = this._where();
				if (where)
					sql += ` WHERE ${where}`;

				if (this._object.return)
					sql += ` RETURNING ${this._return()}`;
				else if (this._object.select)
					sql += ` RETURNING ${this._select()}`;
			} else {
				let join = this._join('LEFT');
				if (join)
					sql += ` ${join}`;

				let where = this._where();
				if (where)
					sql += ` WHERE ${where}`;

				let group = this._group();
				if (group)
					sql += ` GROUP BY ${group}`;
				let having = this._having();
				if (having)
					sql += ` HAVING ${having}`;
				let order = this._order();
				if (order)
					sql += ` ORDER BY ${order}`;
				if (this._object.limit)
					sql += ` LIMIT ${this._object.limit}`;
				// else if (this._object.first)
				// 	sql += ` LIMIT 1`;
				if (this._object.offset)
					sql += ` OFFSET ${this._object.offset}`;
			}

			if (Array.isArray(this._object.table))
				this._object.table.forEach((table, index) => {
					let alias = typeof (table) == 'object' ? Object.keys(table)[0] : table;
					sql = sql.replace(new RegExp(`\\$${index + 1}\\.`, 'g'), `${alias}.`);
				});

			if (this._object.exists)
				sql = `EXISTS(${sql})`;

			return sql;
		}

		print() {
			console.log(this.build());
		}

		toString() {
			return this.build();
		}
	}
})
