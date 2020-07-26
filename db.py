from db_api import *
import csv
import json
import os


@dataclass_json
@dataclass
class DBTable(DBTable):
    name: str
    fields: List[DBField]
    key_field_name: str

    def count(self) -> int:
        raise NotImplementedError

    def insert_record(self, values: Dict[str, Any]) -> None:
        raise NotImplementedError

    def delete_record(self, key: Any) -> None:
        raise NotImplementedError

    def delete_records(self, criteria: List[SelectionCriteria]) -> None:
        raise NotImplementedError

    def get_record(self, key: Any) -> Dict[str, Any]:
        raise NotImplementedError

    def update_record(self, key: Any, values: Dict[str, Any]) -> None:
        raise NotImplementedError

    def query_table(self, criteria: List[SelectionCriteria]) \
            -> List[Dict[str, Any]]:
        raise NotImplementedError

    def create_index(self, field_to_index: str) -> None:
        raise NotImplementedError


@dataclass_json
@dataclass
class DataBase(DataBase):
    __NUM_TABLE__ = 0

    def create_table(self,
                     table_name: str,
                     fields: List[DBField],
                     key_field_name: str) -> DBTable:

        fields_names = [x.name for x in fields]
        fields_types = [str(x.type) for x in fields]

        # validation of key
        if key_field_name not in fields_names:
            raise ValueError("Key isn't in fields names")

        with open(f"db_files/{table_name}.csv", "w") as csv_file:
            csv_writer = csv.writer(csv_file)
            csv_writer.writerow(fields_names)
            DataBase.__NUM_TABLE__ += 1

        with open("db_files/table_info.json", "w") as json_file:
            if DataBase.__NUM_TABLE__ == 1:
                json_data = {}
            else:
                json_data = json.load(json_file)

            json_data[table_name] = {
                "key": key_field_name,
                "names": fields_names,
                "types": fields_types
                # and more...
            }
            json.dump(json_data, json_file)

        return DBTable(table_name, fields, key_field_name)

    def num_tables(self) -> int:
        return DataBase.__NUM_TABLE__

    def get_table(self, table_name: str) -> DBTable:
        with open("db_files/table_info.json", "r") as json_file:
            json_data = json.load(json_file)
            try:
                table = json_data[table_name]
                return DBTable(table_name,
                               [DBField(_name, type(_type)) for _name, _type in zip(table["names"], table["types"])],
                               table["key"])

            except KeyError:
                raise ValueError("Table is not exists")

    def delete_table(self, table_name: str) -> None:
        with open("db_files/table_info.json", "r") as json_file:
            json_data = json.load(json_file)
            try:
                del json_data[table_name]
                json.dump(json_data, json_file)
                os.remove(f"{table_name}.csv")

            except KeyError:
                raise ValueError("Table is not exists")

    def get_tables_names(self) -> List[Any]:
        with open("db_files/table_info.json", "r") as json_file:
            json_data = json.load(json_file)
            return json_data.keys()

    def query_multiple_tables(
            self,
            tables: List[str],
            fields_and_values_list: List[List[SelectionCriteria]],
            fields_to_join_by: List[str]
    ) -> List[Dict[str, Any]]:
        raise NotImplementedError
