<?php

	function dic_map($dic, callable $map) {
		return array_map(function($key) use ($map, $dic) {
			return $map($key, $dic[$key]);
		}, array_keys($dic));
	}

	// SQL for Jerries

	class QueryChain {
		public $table = NULL;		// str
		public $select = NULL;		// str, arr
		public $insert = NULL;		// str, dic
		public $update = NULL;		// str, dic
		public $delete = NULL;		// any
		public $where = NULL;		// str, dic
		public $order = NULL;		// str, arr, dic
		public $limit = NULL;		// int
		public $addColumn = NULL;	// dic
		public $dropColumn = NULL;	// arr

		public function __construct($table = NULL) {
			$this->table = $table;
			$this->select = "*";
		}

		public function table($table) {
			$this->table = $table;
			return $this;
		}

		public function select($columns = NULL) {
			if ($columns === NULL)
				$select = "*";
			else if (gettype($columns) == "string")
				$select = $columns;
			else if (gettype($columns) == "array")
				$select = implode(", ", $columns);

			$this->select = $select;
			return $this;
		}

		public function insert($columns) {
			$keys = implode(", ", array_keys($columns));
			$vals = implode(", ", array_map(function ($val) {
				return gettype($val) == "string" ? ($val == "now()" ? $val : '"'.$val.'"') : ($val === NULL ? "NULL" : $val);
			}, array_values($columns)));
			$insert = "(".$keys.") VALUES (".$vals.")";

			$this->insert = $insert;
			return $this;
		}

		public function update($columns) {
			$update = implode(", ", dic_map($columns, function ($key, $val) {
				$tmp = gettype($val) == "string"
					? ($val == "now()"
						? $val
						: '"'.$val.'"')
					: ($val === NULL
						? "NULL"
						: $val);
				return $key."=".$tmp;
			}));

			$this->update = $update;
			return $this;
		}

		public function delete() {
			$this->delete = "*";
			return $this;
		}

		public function where($where) {
			if (gettype($where) == "array")
				$where = implode(" AND ", dic_map($where, function($key, $val) {
					$type = gettype($val);
					return $key." = ".($type == "string" ? '"'.$val.'"' : ($type == "object" && get_class($val) == "QueryChain" ? '('.$val->build().')' : ($val === NULL ? "NULL" : $val)));
				}));

			$this->where = $where;
			return $this;
		}

		public function order($order) {
			if (gettype($order) == "array")
				$order = implode(", ", dic_map($order, function ($key, $val) {
					return $key." ".$val;
				}));

			$this->order = $order;
			return $this;
		}

		public function limit($limit) {
			$this->limit = $limit;
			return $this;
		}

		public function addColumn($columns) {
			if (gettype($columns) == 'array' && count($columns) > 0)
				$columns = implode(', ', dic_map($columns, function ($key, $val) {
					return 'ADD COLUMN '.$key.' '.$val;
				}));

			$this->addColumn = $columns;
			return $this;
		}

		public function dropColumn($columns) {
			if (gettype($columns) == 'array' && count($columns) > 0)
				$columns = implode(', ', array_map(function ($val) {
					return 'DROP COLUMN '.$val;
				}, $columns));

			$this->dropColumn = $columns;
			return $this;
		}

		public function build() {
			$sql = "SELECT ".$this->select;
			if ($this->table)
				$sql .= " FROM ".$this->table;

			if ($this->insert)
				return "INSERT INTO ".$this->table." ".$this->insert;
			else if ($this->update)
				$sql = "UPDATE ".$this->table." SET ".$this->update;
			else if ($this->delete)
				$sql = "DELETE FROM ".$this->table;
			else if ($this->addColumn)
				return 'ALTER TABLE '.$this->table.' '.$this->addColumn;
			else if ($this->dropColumn)
				return 'ALTER TABLE '.$this->table.' '.$this->dropColumn;

			if ($this->where)
				$sql .= " WHERE ".$this->where;
			if ($this->order)
				$sql .= " ORDER BY ".$this->order;
			if ($this->limit)
				$sql .= " LIMIT ".$this->limit;

			return $sql;
		}
/*
		public function query($db) {
			return $db->query($this->build());
		}
*/
		public function fetch($db) {
			$res = $db->query($this->build());

			if (gettype($res) != "object" || get_class($res) != "mysqli_result")
				return $res;


			$arr = array();

			while ($row = $res->fetch_assoc())
				$arr[] = $row;

			$res->free();

			return $arr;
		}

		public function print($db = NULL, $format = NULL) {
			if ($db == NULL)
				echo($this->build());
			else if ($format == "json")
				echo(json_encode($this->fetch($db)));
			else if ($format == "html") {
				$rows = $this->fetch($db);

				$cols = [];
				foreach ($rows as $row)
					foreach (array_keys($row) as $col)
						if (!in_array($col, $cols))
							$cols[] = $col;

				echo "<table>";

				echo "<tr>";
				foreach ($cols as $col)
					echo "<th>".$col."</th>";
				echo "</tr>";

				foreach ($rows as $row) {
					echo "<tr>";

					foreach ($cols as $col)
						echo (is_numeric($row[$col]) ? "<td align=\"right\">" : "<td>").$row[$col]."</td>";

					echo "</tr>";
				}

				echo "</table>";
			} else
				print_r($this->fetch($db));
		}

		public function __toString() {
			return $this->build();
		}

		public static function query($table = NULL) {
			return (new QueryChain($table));
		}
	}

	function query($table = NULL) {
		return new QueryChain($table);
	}

?>