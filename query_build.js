module.exports = class QueryBuild {
	constructor(log) {
		this._object = {};

		this._log = typeof (log) == 'string' ? {
			doc: log.includes('doc'),
			err: log.includes('err'),
			obj: log.includes('obj'),
			sql: log.includes('sql')
		} : (log || {});
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
			? this._object.table.map(el => {
				return el instanceof Object
					? el.select
						?`(${new QueryBuild(this._log).query(null, el).build()}) query`
						: Object.keys(el).map(key => `${el[key]} AS ${key}`)
					: el
			}).join(', ')
			: this._object.table instanceof Object
				? this._object.table.select
					? `(${new QueryBuild(this._log).query(null, this._object.table).build()}) query`
					: Object.keys(this._object.table).map(key => `${this._object.table[key]} AS ${key}`)
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
						if (val instanceof QueryBuild)
							val = `(${val.build()})`;
						else if (val instanceof Object)
							val = `(${new QueryBuild(this._log).query(val.table, val).build()})`;
						else if (typeof(val) == 'string' && !val.match(/\W/))
							val = `"${val}"`;

						return `${val}`;
					}).join(', ')
					: obj instanceof Object
						? Object.keys(obj).filter(key => key != '_' && !(key == '$' && obj[obj[key]] !== undefined)).map(key => {
							let val = obj[key];

//							if (key == '$' && obj[val] !== undefined)
//								return undefined;

							if (val instanceof QueryBuild)
								val = `(${val.build()})`;
							else if (val instanceof Object)
								val = /*Object.keys(val).every(key => key == '$')
									? val.$
									:*/ `(${new QueryBuild(this._log).query(val.table, val).build()})`;
							else if (typeof(val) == 'string' && !val.match(/\W/))
								val = `"${val}"`;

							return key == '$' ? val : `${val} AS ${key}`;
						})/*.filter(el => el !== undefined)*/.join(', ')
						: typeof(obj) == 'string' && !obj.match(/\W/) ? `"${obj}"` : obj;
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
						? Object.keys(obj).filter(key => key != '_' && !(key == '$' && obj[obj[key]] !== undefined)).map(key => {
							let val = obj[key];

							if (typeof (val) == 'string' && !val.match(/\W/))
								val = `"${val}"`;

							return key == '$' ? val : `${val} AS ${key}`;
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

				if (val == undefined)
					val = 'DEFAULT';
				else if (val == null)
					val = 'NULL';
				else if (typeof(val) == 'string' && val != 'NOW()' && !val.match(/^\$\d+\./))
					val = `'${val.replace(/\'/g, '\'\'')}'`;
				else if (val instanceof Object && Object.keys(val).includes('$'))
					val = val.$;
				else if (val instanceof QueryBuild)
					val = `(${val.build()})`;
				else if (val instanceof Object)
					val = `(${new QueryBuild(this._log).query(null, val).build()})`;

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
				else if (val instanceof QueryBuild)
					val = `(${val.build()})`;
				else if (val instanceof Object)
					val = `(${new QueryBuild(this._log).query(null, val).build()})`;

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

	_where(where) {
		let first = where === undefined;

		if (first)
			where = this._object.where;

		if (!where)
			return null;

		let args = (Array.isArray(where) ? where : [where]).map(arg => {
			if (Array.isArray(arg))
				return arg.length ? this._where(arg) : null;
			else if (arg instanceof Object)
				return Object.keys(arg).map(key => {
					var val = arg[key];

					if (key == '$') {
						return this._where(val);
					} else if (Array.isArray(val)) {
						let arr = val.map(tmp => typeof (tmp) == 'string' && tmp != 'NOW()' && !tmp.match(/^\$\d+\./)
							? `'${tmp.replace(/\'/g, '\'\'')}'`
							: tmp instanceof Object && Object.keys(tmp).includes('$')
								? tmp.$
								: tmp);

						return arr.length > 0 ? `${key} IN ( ${arr.join(', ')} )` : 'FALSE';
					} else if (val instanceof Object) {
						if (Object.keys(val).includes('$'))
							return `${key} = ${this._where(val)}`;

						let tmp = val instanceof QueryBuild ? val : new QueryBuild(this._log).query(null, val).build();

						return val.limit == 1 || val.first == 1 ? `${key} = (${tmp})` : `${key} IN (${tmp})`
					}

					/*if (val == null)
						val = 'NULL';
					else */if (typeof(val) == 'string' && val != 'NOW()' && !val.match(/^\$\d+\./))
						val = `'${val.replace(/\'/g, '\'\'')}'`;
					else if (val instanceof Object && Object.keys(val).includes('$'))
						val = this._where(val);
					else if (val instanceof QueryBuild)
						val = `(${val.build()})`;

					return val == null ? `${key} IS NULL` : `${key} = ${val}`;
				}).join(' AND ');
			else
				return arg;
		});
//		let args = proc(where);

		return first
			? args.join(' OR ')
			: `(${args.join(' OR ')})`;
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

					if ([ 'number', 'boolean' ].includes(typeof (val)))
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

	build() {
		if (this._object.query)
			return this._object.query;

		this._object.queryValues = null;

		var sql = null;

		if (this._object.insert) {
			sql = `INSERT INTO ${this._table()} ${this._insert()}`;

			if (this._object.return)
				sql += ` RETURNING ${this._return()}`;

			if (this._object.conflict) {
				sql += ` ON CONFLICT (${this._conflict()})`;
				if (this._object.update)
					sql += ` DO UPDATE SET ${this._update()}`;
				let where = this._where();
				if (where)
					sql += ` WHERE ${where}`;
				if (!this._object.update)
					sql += ' DO NOTHING';
			}

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