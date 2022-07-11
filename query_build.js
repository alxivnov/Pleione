
(function (e, t) {
	typeof exports == 'object' && typeof module != 'undefined'
		? module.exports = t()
		: typeof define == 'function' && define.amd
			? define(t)
			: e.QueryBuild = t()
})(this, function () {
	'use strict'

	return class QueryBuild {
		constructor(log) {
			this._object = {};

			this._log = typeof (log) == 'string' ? {
				doc: log.includes('doc'),
				err: log.includes('err'),
				obj: log.includes('obj'),
				sql: log.includes('sql')
			} : (log || {});
		}

		_spreadIfArray(method, values) {
			if (Array.isArray(values))
				method(...values);
			else
				method(values);
		}

		_joinIfArray(array, separator = ', ') {
			return Array.isArray(array) ? array.join(separator) : array;
		}

		query(query = null, values = null) {
			if (query && (Array.isArray(query) || typeof (query) == 'object')) {
				if (Array.isArray(query) && query.every(el => typeof (el) == 'string' && !el.includes(' ')))
					this._object.table = query;
				else
					this._object.query = query;//'BEGIN;\n' + query.map(obj => (obj instanceof QueryBuild ? obj : new QueryBuild(this._log).query(obj, values)).build()).join('; ') + ';\nCOMMIT';
				this._object.queryValues = values;
			} else if (query && query.includes(' ')) {
				this._object.query = query;
				this._object.queryValues = values;
			} else if (values && values instanceof Object) {
				this.table(query);

				if (values.select)
					this._spreadIfArray(this.select.bind(this), values.select);
				else if (values.insert)
					this._spreadIfArray(this.insert.bind(this), values.insert);
				else if (values.update)
					this._spreadIfArray(this.update.bind(this), values.update);
				else if (values.delete)
					this.delete(values.delete);

				if (values.table)
					this.table(values.table);
				if (values.join)
					this._spreadIfArray(this.join.bind(this), values.join);
				if (values.where)
					this._spreadIfArray(this.where.bind(this), values.where);
				if (values.group)
					this._spreadIfArray(this.group.bind(this), values.group);
				if (values.distinct)
					this._spreadIfArray(this.distinct.bind(this), values.distinct);
				if (values.order)
					this._spreadIfArray(this.order.bind(this), values.order);
				if (values.limit)
					this.limit(values.limit);
				if (values.offset)
					this.offset(values.offset);
				if (values.first)
					this.limit(0);

				if (values.exists)
					this.exists();
			} else {
				this._object.table = query;
//				this._object.select = '*';
			}

			return this;
		}

		table(table) {
			this._object.table = table;

			return this;
		}

		_table() {
			return Array.isArray(this._object.table)
				? this._object.table.map(el => {
					return el instanceof Object
						? el.select
							? `(${new QueryBuild(this._log).query(null, el).build()}) query`
							: Object.keys(el)
								.filter(key => !(this._object.join && this._object.join.some(join => Object.keys(join).includes(key))))
								.map(key => `${el[key]} AS ${key}`)
						: el
				}).filter(el => !Array.isArray(el) || el.length).join(', ')
				: this._object.table instanceof Object
					? this._object.table.select
						? `(${new QueryBuild(this._log).query(null, this._object.table).build()}) query`
						: Object.keys(this._object.table)
							.filter(key => !(this._object.join && this._object.join.some(join => Object.keys(join).includes(key))))
							.map(key => `${this._object.table[key] instanceof QueryBuild ? '(' + this._object.table[key] + ')' : this._object.table[key]} AS ${key}`)
							.join(', ')
					: this._object.table;
		}

		join(...args) {
			this._object.join = args.filter(arg => !!arg);

			return this;
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
								from: `(${new QueryBuild(this._log).query(null, val.from).build()})`,
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

		exists() {
			this._object.exists = true;

			return this;
		}

		select(...cols) {
			this._object.select = cols && cols.every(el => el) ? cols : [];

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
							if (val instanceof QueryBuild)
								val = `(${val.build()})`;
							else if (val instanceof Object)
								val = `(${new QueryBuild(this._log).query(val.table, val).build()})`;
							else if (typeof (val) == 'string' && !val.match(/\W/))
								val = `"${val}"`;

							return `${val}`;
						}).join(', ')
						: obj instanceof Object
							? Object.keys(obj).filter(key => /*key != '_' &&*/ !([ '$', '_' ].includes(key) && obj[obj[key]] !== undefined)).map(key => {
								let val = obj[key];

//							if (key == '$' && obj[val] !== undefined)
//								return undefined;

								if (val instanceof QueryBuild)
									val = `(${val.build()})`;
								else if (Array.isArray(val))
									val = `COALESCE(${val.join(', ')})`;
								else if (val instanceof Object)
									val = /*Object.keys(val).every(key => key == '$')
									? val.$
									:*/ `(${new QueryBuild(this._log).query(val.table, val).build()})`;
								else if (typeof (val) == 'string' && !val.match(/\W/))
									val = `"${val}"`;

								return [ '$', '_' ].includes(key) ? val : `${val} AS ${key}`;
							})/*.filter(el => el !== undefined)*/.join(', ')
							: typeof (obj) == 'string' && !obj.match(/\W/) ? `"${obj}"` : obj;
				}).join(', ');
		}

		return(...cols) {
			this._object.return = cols && cols.every(el => el) ? cols : [];

			return this;
		}

		_return() {
			let cols = this._object.return;

			if (cols.length == 0)
				return "*";
			else
				return cols.map(obj => {
					return Array.isArray(obj)
						? obj.map(val => {
							if (typeof (val) == 'string' && !val.match(/\W/))
								val = `"${val}"`;

							return `${val}`;
						}).join(', ')
						: obj instanceof Object
							? Object.keys(obj).filter(key => /*key != '_' &&*/ !([ '$', '_' ].includes(key) && obj[obj[key]] !== undefined)).map(key => {
								let val = obj[key];

								if (typeof (val) == 'string' && !val.match(/\W/))
									val = `"${val}"`;

								return [ '$', '_' ].includes(key) ? val : `${val} AS ${key}`;
							}).join(', ')
							: typeof (obj) == 'string' && !obj.match(/\W/) ? `"${obj}"` : obj;
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

					if (val === undefined)
						val = 'DEFAULT';
					else if (val === null)
						val = 'NULL';
					else if (typeof (val) == 'string' && val != 'NOW()' && !val.match(/^\$\d+\./))
						val = `'${val.replace(/\'/g, '\'\'')}'`;
					else if (val instanceof Object && Object.keys(val).includes('$'))
						val = val.$;
					else if (val instanceof QueryBuild)
						val = `(${val.build()})`;
					else if (val instanceof Object && (val.select || val.return || val.tables || val.columns || val.table || val.query))
						val = `(${new QueryBuild(this._log).query(null, val).build()})`;
					else if (val instanceof Object)
						val = `'${JSON.stringify(val).replace(/'/g, `''`)}'`;

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

		_update(update) {
			let vals = update || this._object.update;

			return vals.map(obj => {
				return obj instanceof Object ? Object.keys(obj).map(key => {
					let val = obj[key];

					if (val === null)
						val = 'NULL'
					else if (typeof (val) == 'string' && val != 'NOW()' && !val.match(/^\$\d+\./))
						val = `'${val.replace(/\'/g, '\'\'')}'`;
					else if (val instanceof Object && Object.keys(val).includes('$'))
						val = val.$;
					else if (val instanceof QueryBuild)
						val = `(${val.build()})`;
					else if (val instanceof Object)
						val = `(${new QueryBuild(this._log).query(null, val).build()})`;

					return `${key}=${val}`;
				}).join(', ') : obj;
			}).join(', ');
		}

		delete(...where) {
			this._object.delete = '*';

			this._object.where = where && where.every(el => el) ? where : null;

			return this;
		}

		where(...where) {
			this._object.where = where && where.every(el => el) ? where : null;

			return this;
		}

		_where(where, separator) {
			if (!where)
				where = this._object.where;

			if (!where)
				return null;

			let args = (Array.isArray(where) ? where : [where]).map(arg => {
				if (Array.isArray(arg))
					return arg.length ? `(${this._where(arg)})` : null;
				else if (arg instanceof Object)
					return Object.keys(arg).filter(key => arg[key] !== undefined).map(key => {
						var val = arg[key];

						if (key == '$') {
							return `(${this._where(val)})`;
						} else if (key == 'AND') {
							return `(${this._where(val, 'AND')})`;
						} else if (key == 'OR') {
							return `(${this._where(val, 'OR')})`;
						} else if (Array.isArray(val)) {
							let arr = val.map(tmp => typeof (tmp) == 'string' && tmp != 'NOW()' && !tmp.match(/^\$\d+\./)
								? `'${tmp.replace(/\'/g, '\'\'')}'`
								: tmp instanceof Object && Object.keys(tmp).includes('$')
									? tmp.$
									: tmp);

							let isNull = arr.includes(null) ? ` OR ${key} IS NULL` : '';
							return arr.length > 0 ? `(${key} IN (${arr.filter(el => el !== null).join(', ')})${isNull})` : 'FALSE';
						} else if (val instanceof Object) {
							let keys = Object.keys(val);

							if (keys.includes('$'))
								return `${key} = (${this._where(val)})`;
							else if (keys.includes('_'))
								return `(${this._where(val._)})`;
							else if (keys.includes('AND'))
								return `(${this._where(val.AND, 'AND')})`;
							else if (keys.includes('OR'))
								return `(${this._where(val.OR, 'OR')})`;

							let tmp = val instanceof QueryBuild ? val : new QueryBuild(this._log).query(null, val).build();

							return `${key} ${val instanceof QueryBuild || val.limit != 1 || val.first != 1 || !val.exists ? 'IN' : '='} (${tmp})`;
						}

					/*if (val === null)
						val = 'NULL';
					else */if (typeof (val) == 'string' && val != 'NOW()' && !val.match(/^\$\d+\./))
							val = `'${val.replace(/\'/g, '\'\'')}'`;
						else if (val instanceof Object && Object.keys(val).includes('$'))
							val = `(${this._where(val)})`;
						else if (val instanceof QueryBuild)
							val = `(${val.build()})`;

						return val === null ? `${key} IS NULL` : `${key} ${val instanceof QueryBuild ? 'IN' : '='} ${val}`;
					}).join(' AND ');
				else
					return arg;
			});
			//		let args = proc(where);

			return args.join(separator ? ` ${separator} ` : ' OR ');
		}

		group(...group) {
			this._object.group = group && group.every(el => el) ? group : null;

			return this;
		}

		_group() {
			let group = this._object.group;
			if (!group)
				return null;

			return group
				.map(el => !el.match(/\W/) ? `"${el}"` : el)
				.join(', ');
		}

		having(...args) {
			this._object.having = args && args.every(el => el) ? args : null;

			return this;
		}

		_having() {
			let having = this._object.having;

			if (!having)
				return null;

			return this._where(this._object.having);
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

						if (typeof (key) == 'string' && !key.match(/\W/))
							key = `"${key}"`;

						if (['number', 'boolean'].includes(typeof (val)))
							val = val > 0 ? 'ASC' : 'DESC';

						return `${key} ${val}`;
					}).join(', ');
				else
					return arg;
			});

			return args.join(', ');
		}

		limit(limit) {
			if (limit === 0/* || limit === undefined*/)
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

			let columns = conflict.filter(el => typeof (el) == 'string' && !el.match(/\s/)).join(', ');
			let where = conflict.filter(el => typeof (el) != 'string' || el.match(/\s/)).map(el => this._where(el)).join(' AND ');

			return `ON CONFLICT (${columns})` + (where ? ` WHERE ${where}` : '');
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

		createDatabase(database, owner) {
			this._object.createDatabase = database;
			this._object.owner = owner;

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

		grant(to, on, privileges = undefined, onModifier = undefined) {
			this._object.query = `GRANT ${this._joinIfArray(privileges) || 'ALL'} ON ${onModifier} ${this._joinIfArray(on)} TO ${this._joinIfArray(to)}`;

			return this;
		}

		revoke(from, on, privileges = undefined, onModifier = undefined) {
			this._object.query = `REVOKE ${this._joinIfArray(privileges) || 'ALL'} ON ${onModifier} ${this._joinIfArray(on)} TO ${this._joinIfArray(from)}`;

			return this;
		}

		tables(...cols) {
			this._object.tables = cols && cols.every(el => el) ? cols : [];

			return this;
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

		build() {
			if (this._object.query != null)
				return this._object.query;

			this._object.queryValues = null;

			var sql = null;

			if (this._object.insert) {
				let insert = this._insert();
				if (!insert.length)
					return null;

				sql = `INSERT INTO ${this._table()} ${insert}`;

				if (this._object.conflict) {
					sql += ` ${this._conflict()}`;
					if (this._object.update)
						sql += ` DO UPDATE SET ${this._update(this._object.update.length ? this._object.update : [ this._object.insert
							.filter(row => row instanceof Object)
							.flatMap(row => Object.keys(row))
							.reduce((cols, col) => {
								if (!cols[col] && !this._object.conflict.includes(col))
									cols[col] = { $: `excluded.${col}` };
								return cols;
							}, {}) ])}`;
					let where = this._where();
					if (where)
						sql += ` WHERE ${where}`;
					if (!this._object.update)
						sql += ' DO NOTHING';
				}

				if (this._object.return)
					sql += ` RETURNING ${this._return()}`;
				else if (this._object.select)
					sql += ` RETURNING ${this._select()}`;

				return sql;
			} else if (this._object.update) {
				let update = this._update();
				if (!update.length)
					return null;

				sql = `UPDATE ${this._table()} SET ${update}`;
			} else if (this._object.delete)
				sql = `DELETE FROM ${this._table()}`;
			else if (this._object.createDatabase)
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
			else {
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
			else if (this._object.first)
				sql += ` LIMIT 1`;
			if (this._object.offset)
				sql += ` OFFSET ${this._object.offset}`;

			if (Array.isArray(this._object.table))
				this._object.table.forEach((table, index) => {
					sql = sql.replace(new RegExp(`\\$${index + 1}\\.`, 'g'), `${table}.`);
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

		serialize() {
			return this._object;
		}
	}
})
