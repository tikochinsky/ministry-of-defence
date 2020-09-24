from db_api import *
import csv
import json
import os
import my_utils
from BTrees.OOBTree import OOBTree


@dataclass_json
@dataclass
class DBTable(DBTable):
    name: str
    fields: List[DBField]
    key_field_name: str

    def __init__(self, name, fields, key_field_name):
        self.btree_index_info = {}
        self.hash_index_info = {}
        self.name = name
        self.fields = fields
        self.key_field_name = key_field_name

        self.create_index(key_field_name, index_type="hash")

    def get_fields_names(self) -> List[str]:
        return [x.name for x in self.fields]

    def get_index_of_field(self, field: str) -> int:
        return self.get_fields_names().index(field)

    def insert_into_index(self, record: List[any], path: str, line: int) -> None:
        for i, field in enumerate(self.get_fields_names()):
            if self.hash_index_info.get(field):
                self.hash_index_info[field][record[i]] = {
                    "path": path,
                    "line": line
                }

            if self.btree_index_info.get(field):
                self.hash_index_info[field].insert((record[i], {
                    "path": path,
                    "line": line
                }))

    def delete_from_index(self, record: List[any], path: str, line: int) -> None:
        for i, field in enumerate(self.get_fields_names()):
            if self.hash_index_info.get(field):
                del self.hash_index_info[field][record[i]]

            if self.btree_index_info.get(field):
                self.hash_index_info[field].pop(record[i], None)

    def count(self) -> int:
        return DataBase.table_info[self.name]["count"]

    def insert_record(self, values: Dict[str, Any]) -> None:
        # num of records > 1000

        # key isn't in the dict keys
        if self.key_field_name not in values.keys():
            raise ValueError("Key is a must field")

        # if there are more fields in values
        if len(set(values.keys()) - set(self.get_fields_names())) != 0:
            raise ValueError("Column name not found")

        # insert according to columns order
        record_to_insert = []
        for name in self.get_fields_names():
            record_to_insert.append(values.get(name))

        with open(f"{DB_ROOT}/{self.name}.csv", "r", newline="") as csv_file:
            csv_reader = csv.reader(csv_file)

            rows = []
            # if key already exists - later via index
            index_of_key = self.get_index_of_field(self.key_field_name)
            for record in csv_reader:
                if record and record[index_of_key] == str(values[self.key_field_name]):
                    raise ValueError("Key already exists")
                rows.append(record)

            inserted = False
            for i, record in enumerate(rows):
                if not record:
                    rows[i] = record_to_insert
                    inserted = True
                    self.insert_into_index(record_to_insert, f"{DB_ROOT}/{self.name}.csv", i)
                    break
            if not inserted:
                rows.append(record_to_insert)
                self.insert_into_index(record_to_insert, f"{DB_ROOT}/{self.name}.csv", len(rows))

        with open(f"{DB_ROOT}/{self.name}.csv", "w", newline="") as csv_file:
            csv_writer = csv.writer(csv_file)
            csv_writer.writerows(rows)

        DataBase.table_info[self.name]["count"] += 1


    def delete_record(self, key: Any) -> None:
        with open(f"{DB_ROOT}/{self.name}.csv", "r") as csv_file:
            csv_reader = csv.reader(csv_file)
            index_of_key = self.get_index_of_field(self.key_field_name)

            found = False
            clean_rows = []
            for record in csv_reader:
                if record and record[index_of_key] == str(key):
                    found = True
                    clean_rows.append([])
                else:
                    clean_rows.append(record)

            if not found:
                raise ValueError("Key not found")

        with open(f"{DB_ROOT}/{self.name}.csv", "w", newline="") as csv_file:
            csv_writer = csv.writer(csv_file)
            csv_writer.writerows(clean_rows)

        DataBase.table_info[self.name]["count"] -= 1

    def delete_records(self, criteria: List[SelectionCriteria]) -> None:
        num_of_removed = 0
        with open(f"{DB_ROOT}/{self.name}.csv", "r") as csv_file:
            csv_reader = csv.reader(csv_file)

            clean_rows = [next(csv_reader)]
            for record in csv_reader:
                if not record:
                    continue
                operation = ""
                for criterion in criteria:
                    index_of_field = self.get_index_of_field(criterion.field_name)
                    if criterion.operator == "=":
                        criterion.operator = "=="
                    if isinstance(criterion.value, str):
                        operation += f"'{record[index_of_field]}' {criterion.operator} '{criterion.value}' and "
                    else:
                        operation += f"{record[index_of_field]} {criterion.operator} {criterion.value} and "

                if not eval(operation[:-4]):
                    clean_rows.append(record)
                else:
                    clean_rows.append([])
                    num_of_removed += 1

        with open(f"{DB_ROOT}/{self.name}.csv", "w", newline="") as csv_file:
            csv_writer = csv.writer(csv_file)
            csv_writer.writerows(clean_rows)

        DataBase.table_info[self.name]["count"] -= num_of_removed

    def get_record(self, key: Any) -> Dict[str, Any]:
        with open(f"{DB_ROOT}/{self.name}.csv", "r") as csv_file:
            csv_reader = csv.reader(csv_file)

            index_of_key = self.get_index_of_field(self.key_field_name)

            next(csv_reader)
            for record in csv_reader:
                if record[index_of_key] == str(key):
                    return dict(zip(self.get_fields_names(), record))

        raise ValueError("The record doesn't exist in the table")

    def update_record(self, key: Any, values: Dict[str, Any]) -> None:
        with open(f"{DB_ROOT}/{self.name}.csv", "r") as csv_file:
            csv_reader = csv.reader(csv_file)
            clean_rows = []
            key_index = self.get_index_of_field(self.key_field_name)
            updated = False
            for record in csv_reader:
                if record and record[key_index] == str(key):
                    updated = True
                    for name in self.get_fields_names():
                        if name in values.keys():
                            record[self.get_index_of_field(name)] = values.get(name)
                clean_rows.append(record)
            if not updated:
                raise ValueError("The record not exist")
        with open(f"{DB_ROOT}/{self.name}.csv", "w", newline="") as csv_file:
            csv_writer = csv.writer(csv_file)
            csv_writer.writerows(clean_rows)

    def query_table(self, criteria: List[SelectionCriteria]) \
            -> List[Dict[str, Any]]:
        result = []

        with open(f"{DB_ROOT}/{self.name}.csv", "r") as csv_file:
            csv_reader = csv.reader(csv_file)
            next(csv_reader)
            for record in csv_reader:
                if not record:
                    continue
                operation = ""
                for criterion in criteria:
                    index_of_field = self.get_index_of_field(criterion.field_name)
                    if criterion.operator == "=":
                        criterion.operator = "=="
                    if isinstance(criterion.value, str):
                        operation += f"'{record[index_of_field]}' {criterion.operator} '{criterion.value}' and "
                    else:
                        operation += f"{record[index_of_field]} {criterion.operator} {criterion.value} and "

                if eval(operation[:-4]):
                    result.append(dict(zip(self.get_fields_names(), record)))

        return result

    def create_index(self, field_to_index: str, index_type="btree") -> None:
        index_of_field = self.get_index_of_field(field_to_index)
        with open(f"{DB_ROOT}/{self.name}.csv", "r") as csv_file:
            csv_reader = csv.reader(csv_file)
            index_values = {}
            next(csv_reader)
            for record in csv_reader:
                if record:
                    index_values[record[index_of_field]] = {
                        "path": "{DB_ROOT}/{self.name}.csv",
                        "line": csv_reader.line_num
                    }

        if index_type == "btree":
            btree_index = OOBTree()
            btree_index.update(index_values)
            self.btree_index_info[field_to_index] = btree_index

        if index_type == "hash":
            self.hash_index_info[field_to_index] = index_values

        else:
            raise ValueError("No such index type")


@dataclass_json
@dataclass
class DataBase(DataBase):
    def __init__(self):
        try:
            if not os.path.exists(DB_ROOT):
                DB_ROOT.mkdir(parents=True, exist_ok=True)

            with open("table_info.json", "r") as json_file:
                DataBase.table_info = json.load(json_file)

        except FileNotFoundError:
            DataBase.table_info = {}

    def create_table(self,
                     table_name: str,
                     fields: List[DBField],
                     key_field_name: str) -> DBTable:

        fields_names = [x.name for x in fields]
        fields_types = [str(x.type) for x in fields]

        # validation of key
        if key_field_name not in fields_names:
            raise ValueError("Key isn't in fields names")

        with open(f"{DB_ROOT}/{table_name}.csv", "w", newline="") as csv_file:
            csv_writer = csv.writer(csv_file)
            csv_writer.writerow(fields_names)

        if table_name in DataBase.table_info.keys():
            raise ValueError("Table already exists")

        DataBase.table_info[table_name] = {
            "key": key_field_name,
            "names": fields_names,
            "types": fields_types,
            "count": 0
            # and more...
        }

        return DBTable(table_name, fields, key_field_name)

    def num_tables(self) -> int:
        return len(DataBase.table_info)

    def get_table(self, table_name: str) -> DBTable:
        try:
            table = DataBase.table_info[table_name]
            return DBTable(table_name,
                           [DBField(_name, my_utils.get_type(_type)) for _name, _type in
                            zip(table["names"], table["types"])],
                           table["key"])

        except KeyError:
            raise ValueError("Table is not exists")

    def delete_table(self, table_name: str) -> None:
        try:
            del DataBase.table_info[table_name]
            os.remove(f"{DB_ROOT}/{table_name}.csv")

        except KeyError:
            raise ValueError("Table does not exist")

    def get_tables_names(self) -> List[Any]:
        return list(DataBase.table_info.keys())

    def query_multiple_tables(
            self,
            tables: List[str],
            fields_and_values_list: List[List[SelectionCriteria]],
            fields_to_join_by: List[str]
    ) -> List[Dict[str, Any]]:
        raise NotImplementedError

    def __del__(self):
        with open("table_info.json", "w") as json_file:
            json.dump(DataBase.table_info, json_file)


# TODO:
# divide tables to files
# maintain index and change our queries to use indexes
# joins
