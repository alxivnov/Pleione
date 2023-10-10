(function (e, t) {
	typeof exports == 'object' && typeof module != 'undefined'
		? module.exports = t()
		: typeof define == 'function' && define.amd
			? define(t)
			: e.QueryChain = t(e)
})(this, function () {
	"use strict";

	// Chains SQL commands into queries for future execution.
	return class QueryChain {
		constructor(opt, obj) {
			this._opt = opt || {};
			this._object = obj || {};
		}

		_clone(full) {
			let opt = typeof (full) == 'object'
				? full
				: { ...this._opt };

			var obj = {};
			if (full === true) {
				obj = { ...this._object };
				if (typeof (obj.query && obj.query._clone) == 'function')
					obj.query = obj.query._clone();
				else if (Array.isArray(obj.query))
					obj.query = obj.query.map(query => typeof (query._clone) == 'function' ? query._clone() : query);
				else if (typeof (obj.query) == 'object')
					obj.query = Object.keys(obj.query).reduce((obj, key) => {
						obj[key] = typeof (obj.query[key]._clone) == 'function' ? obj.query[key]._clone : obj.query[key];
						return obj;
					}, {});
			}

			return new this.constructor(opt, obj);
		}

		_spreadIfArray(method, values) {
			if (Array.isArray(values))
				return method(...values);
			else
				return method(values);
		}

		_joinIfArray(array, separator = ', ') {
			return Array.isArray(array) ? array.join(separator) : array;
		}

		query(query = null, values = null) {
			var clone = this._clone(true);

			if (query && (Array.isArray(query) || typeof (query) == 'object')) {
				if (Array.isArray(query) && query.every(el => typeof (el) == 'string' && !el.includes(' '))) {
					clone._object.table = query;
					clone._object.queryValues = values;
				} else {
					// clone._object.query = query;//'BEGIN;\n' + query.map(obj => (obj instanceof QueryBuild ? obj : new QueryBuild().query(obj, values)).build()).join('; ') + ';\nCOMMIT';
					// clone._opt = this._merge(values, clone._opt);
				}
			} else if (query && (query.includes(' ') || query.includes(';'))) {
				clone._object.query = query;
				clone._object.queryValues = values;
			} else if (values && values instanceof Object) {
				clone = clone.table(query);

				if (values.select)
					clone = this._spreadIfArray(this.select.bind(clone), values.select);
				else if (values.insert)
					clone = this._spreadIfArray(this.insert.bind(clone), values.insert);
				else if (values.update)
					clone = this._spreadIfArray(this.update.bind(clone), values.update);
				else if (values.delete)
					clone = clone.delete(values.delete);

				if (values.table)
					clone = clone.table(values.table);
				if (values.join)
					clone = this._spreadIfArray(this.join.bind(clone), values.join);
				if (values.where)
					clone = this._spreadIfArray(this.where.bind(clone), values.where);
				if (values.group)
					clone = this._spreadIfArray(this.group.bind(clone), values.group);
				if (values.distinct)
					clone = this._spreadIfArray(this.distinct.bind(clone), values.distinct);
				if (values.order)
					clone = this._spreadIfArray(this.order.bind(clone), values.order);
				if (values.limit)
					clone = clone.limit(values.limit);
				if (values.offset)
					clone = clone.offset(values.offset);
				if (values.first)
					clone = clone.limit(0);

				if (values.exists)
					clone = clone.exists();
			} else {
				clone._object.table = query;
//				clone._object.select = '*';
			}

			return clone;
		}

		table(table) {
			let clone = this._clone(true);
			clone._object.table = table;

			return clone;
		}

		join(...args) {
			let clone = this._clone(true);
			clone._object.join = args.filter(arg => !!arg);

			return clone;
		}

		exists() {
			let clone = this._clone(true);
			clone._object.exists = true;

			return clone;
		}

		select(...cols) {
			let clone = this._clone(true);
			clone._object.select = cols && cols.every(el => el) ? cols : [];

			return clone;
		}

		return(...cols) {
			let clone = this._clone(true);
			clone._object.return = cols && cols.every(el => el) ? cols : [];

			return clone;
		}

		insert(...rows) {
			let clone = this._clone(true);
			clone._object.insert = rows && rows.every(el => el) ? rows : null;

			return clone;
		}

		update(...vals) {
			let clone = this._clone(true);
			clone._object.update = vals && vals.every(el => el) ? vals : null;

			return clone;
		}

		delete(...where) {
			let clone = this._clone(true);
			clone._object.delete = '*';

			if (where && where.length && where.every(el => el))
				clone._object.where = where;

			return clone;
		}

		where(...where) {
			let clone = this._clone(true);
			clone._object.where = where && where.every(el => el) ? where : null;

			return clone;
		}

		group(...group) {
			let clone = this._clone(true);
			clone._object.group = group && group.every(el => el) ? group : null;

			return clone;
		}

		having(...args) {
			let clone = this._clone(true);
			clone._object.having = args && args.every(el => el) ? args : null;

			return clone;
		}

		distinct(...distinct) {
			let clone = this._clone(true);
			clone._object.distinct = distinct && distinct.every(el => el) ? distinct : null;

			return clone;
		}

		order(...order) {
			let clone = this._clone(true);
			clone._object.order = order && order.every(el => el) ? order : null;

			return clone;
		}

		limit(limit) {
			let clone = this._clone(true);
			clone._object.limit = limit;

			return clone;
		}

		offset(offset) {
			let clone = this._clone(true);
			clone._object.offset = offset;

			return clone;
		}

		conflict(...conflict) {
			let clone = this._clone(true);
			clone._object.conflict = conflict && conflict.every(el => el) ? conflict.flatMap(el => Array.isArray(el) ? el : [ el ]) : null;

			return clone;
		}

		// count(col) {
		// 	let clone = this._clone(true);
		// 	clone._object.count = col || '*';

		// 	return clone;
		// }

		createDatabase(database, owner) {
			let clone = this._clone(true);
			clone._object.createDatabase = database;
			clone._object.owner = owner;

			return clone;
		}

		dropDatabase(database) {
			let clone = this._clone(true);
			clone._object.dropDatabase = database;

			return clone;
		}

		createUser(username, password) {
			let clone = this._clone(true);
			clone._object.query = `CREATE USER ${username} WITH ENCRYPTED PASSWORD '${password}'`;

			return clone;
		}

		dropUser(username) {
			let clone = this._clone(true);
			clone._object.query = `DROP USER ${username}`;

			return clone;
		}

		grant(to, privileges = undefined, on = undefined, onModifier = undefined) {
			let query = `GRANT ${this._joinIfArray(privileges) || 'ALL'}`;
			if (on || onModifier) {
				query += ' ON';
				if (onModifier)
					query += ` ${onModifier}`;
				let onString = this._joinIfArray(on);
				if (onString)
					query += ` ${onString}`;
			}
			query += ` TO ${this._joinIfArray(to)}`;

			let clone = this._clone(true);
			clone._object.query = query;

			return clone;
		}

		revoke(from, privileges = undefined, on = undefined, onModifier = undefined) {
			let query = `REVOKE ${this._joinIfArray(privileges) || 'ALL'}`;
			if (on || onModifier) {
				query += ' ON';
				if (onModifier)
					query += ` ${onModifier}`;
				let onString = this._joinIfArray(on);
				if (onString)
					query += ` ${onString}`;
			}
			query += ` FROM ${this._joinIfArray(from)}`;

			let clone = this._clone(true);
			clone._object.query = query;

			return clone;
		}

		tables(...cols) {
			let clone = this._clone(true);
			clone._object.tables = cols && cols.every(el => el) ? cols : [];

			return clone;
		}

		columns(...cols) {
			let clone = this._clone(true);
			clone._object.columns = cols && cols.every(el => el) ? cols : [];

			return clone;
		}

		// WARNING: addColumn
		addColumn(...cols) {
			let clone = this._clone(true);
			clone._object.addColumn = cols/*.map(col => {
			return col;
		})*/.join(', ');

			return clone;
		}

		// WARNING: dropColumn
		dropColumn(...cols) {
			let clone = this._clone(true);
			clone._object.dropColumn = cols/*.map(col => {
			return col;
		})*/.join(', ');

			return clone;
		}

		begin() {
			let clone = this._clone(true);
			clone._object.query = 'BEGIN';

			return clone;
		}

		commit() {
			let clone = this._clone(true);
			clone._object.query = 'COMMIT';

			return clone;
		}

		rollback() {
			let clone = this._clone(true);
			clone._object.query = 'ROLLBACK';

			return clone;
		}

		serialize() {
			return this._object;
		}
	}
})
