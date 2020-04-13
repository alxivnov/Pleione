class QueryChain:
	def __init__(self, table = None):
		self._dic = {
			"select" : "*",
			"table" : table
		}

	def build(self):
		params = {}

		if self._dic.get("insert"):
			sql = "INSERT INTO {} ({}) VALUES ({})"
			sql = sql.format(self._dic["table"], ", ".join(["`{}`".format(i) for i in self._dic["insert"].keys()]), ", ".join(["%({})s".format(i) for i in self._dic["insert"].keys()]))
			params.update(self._dic["insert"])
		elif self._dic.get("update"):
			sql = "UPDATE {} SET {}"
			sql = sql.format(self._dic["table"], ", ".join(["`{0}`=%({0})s".format(i) for i in self._dic["update"].keys()]))
			params.update(self._dic["update"])
		elif self._dic.get("delete"):
			sql = "DELETE FROM {}"
			sql = sql.format(self._dic["table"])
		else:
			sql = "SELECT {}"
			if self._dic.get("select"):
				if isinstance(self._dic["select"], list):
					sql = sql.format(", ".join([(i if "*" in i or "(" in i or " AS " in i else "`{}`".format(i)) for i in self._dic["select"]]))
				else:
					sql = sql.format(self._dic["select"])
			else:
				sql = sql.format("*")
			if self._dic.get("table"):
				sql = "{} FROM {}".format(sql, self._dic["table"])

		if self._dic.get("where"):
			if isinstance(self._dic["where"], dict):
				tmp = " AND ".join(["`{0}`=%({0})s".format(i) for i in self._dic["where"].keys()])
				params.update(self._dic["where"])
			else:
				tmp = self._dic["where"]

			sql = "{} WHERE {}".format(sql, tmp)
		if self._dic.get("group"):
			if isinstance(self._dic["group"], list):
				tmp = ", ".join(["`{}`".format(i) for i in self._dic["group"]])
			else:
				tmp = self._dic["group"]

			sql = "{} GROUP BY {}".format(sql, tmp)
		if self._dic.get("order"):
			if isinstance(self._dic["order"], dict):
				tmp = ", ".join(["`{}` {}".format(i, self._dic["order"][i]) for i in self._dic["order"].keys()])
			elif isinstance(self._dic["order"], list):
				tmp = ", ".join(["`{}`".format(i) for i in self._dic["order"]])
			else:
				tmp = self._dic["order"]

			sql = "{} ORDER BY {}".format(sql, tmp)
		if self._dic.get("limit"):
			sql = "{} LIMIT {}".format(sql, self._dic["limit"])

		for key, val in params.items():
			if hasattr(val, "item") and callable(getattr(val, "item")):
				params[key] = val.item()

		return { "sql" : sql, "params" : params }

	def fetch(self, db = None, params = None):
		sql = self.build()
		if params:
			sql["params"].update(params)

		result = None
		if db and hasattr(db, "execute") and callable(getattr(db, "execute")):
			db.execute(sql["sql"], sql["params"])
			if sql["sql"].startswith("SELECT"):
				result = db.fetchall()
			return result
		elif db and hasattr(db, "cursor") and callable(getattr(db, "cursor")):
			cursor = db.cursor()
			cursor.execute(sql["sql"], sql["params"])
			if sql["sql"].startswith("SELECT"):
				result = cursor.fetchall()
			elif sql["sql"].startswith("INSERT") or sql["sql"].startswith("UPDATE") or sql["sql"].startswith("DELETE"):
				db.commit()
			cursor.close()
			return result
		else:
			return sql

	def table(self, table):
		self._dic["table"] = table

		return self

	def select(self, cols = None):
		self._dic["select"] = cols

		return self

	def insert(self, rows):
		self._dic["insert"] = rows

		return self

	def update(self, cols):
		self._dic["update"] = cols

		return self

	def delete(self):
		self._dic["delete"] = "*"

		return self

	def where(self, where):
		self._dic["where"] = where

		return self

	def group(self, group):
		self._dic["group"] = group

		return self

	def order(self, order):
		self._dic["order"] = order

		return self

	def limit(self, limit):
		self._dic["limit"] = limit

		return self
