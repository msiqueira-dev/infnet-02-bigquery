import os
from google.cloud import bigquery
from google.oauth2 import service_account
from google.cloud import exceptions

class Connector_BigQuery:

    Authenticated = False
    Client_Bq = None
    Credentials = None
    Json_Key_Path = None
    Project_Id = None

    @classmethod
    def __init__(self, json_key_path, project_id):
        if project_id:
            self.Project_Id = project_id
        if json_key_path:
            self.Json_Key_Path = json_key_path

    def authenticate(self):
        if self.Project_Id is None:
            print("authenticate: Project_Id not set")
            return None
        os.environ.setdefault("GCLOUD_PROJECT", self.Project_Id)
        try:
            self.Credentials = service_account.Credentials.from_service_account_file(self.Json_Key_Path)
            self.Client_Bq = bigquery.Client(credentials=self.Credentials, project=self.Credentials.project_id)
            if self.Client_Bq:
                self.Authenticated = True
                return True
        except FileNotFoundError as e:
            msg = f"BigQuery Service Account JSON File {self.Json_Key_Path} not found"
            print(msg)
        except Exception as e:
            print(e)
        return None
    
    def create_dataset(self, dataset_id):
        if self.Client_Bq is None:
            print("create_dataset: Client_Bq not set")
            return None
        elif self.Project_Id is None:
            print("create_dataset: Project_Id not set")
            return None
        elif dataset_id is None:
            print("create_dataset: dataset_id not set")
            return None
        dataset = bigquery.Dataset(f"{self.Project_Id}.{dataset_id}")
        try:
            #dataset.location = "US"
            dataset = self.Client_Bq.create_dataset(dataset, timeout=30)
            if dataset:
                return True
        except exceptions.Forbidden:
            msg = f"BigQuery Account lacks permissions to create datasets in {self.Project_Id}"
            print(msg)
        except Exception as e:
            print(type(e))
            print(e)
        return None
    
    def create_table(self, dataset_id, field_list, table_id, partition_field=None):
        if self.Client_Bq is None:
            print("create_table: Client_Bq not set")
            return None
        elif self.Project_Id is None:
            print("create_table: Project_Id not set")
            return None
        elif dataset_id is None:
            print("create_table: dataset_id not set")
            return None
        elif field_list is None:
            print("create_table: field_list not set")
            return None
        elif type(field_list) is not list:
            print("create_table: field_list is not a list")
            return None
        elif table_id is None:
            print("create_table: table_id not set")
            return None
        list_field_schema = []
        for field in field_list:
            if "type" not in field:
                field_type = "string"
            else:
                field_type = field["type"]
                if "str" == str(field_type).lower() or "char" in str(field_type).lower():
                    field_type = "string"
                elif "bit" in str(field_type).lower():
                    field_type = "bool"
                elif "int" == str(field_type).lower():
                    field_type = "integer"
                elif "bigint" == str(field_type).lower():
                    field_type = "int64"
                elif "float" == str(field_type).lower():
                    field_type = "float64"
            if "required" not in str(field).lower():
                field_required = False
            else:
                field_required = field["required"]
                if str(field_required).lower() == "true":
                    field_required = True
                else:
                    field_required = False
            if field_required:
                bq_field = bigquery.SchemaField(str(field["name"]).lower(), field_type, mode="REQUIRED")
            else:
                bq_field = bigquery.SchemaField(str(field["name"]).lower(), field_type)
            list_field_schema.append(bq_field)
        try:
            table = bigquery.Table(f"{self.Project_Id}.{dataset_id}.{table_id}", schema=list_field_schema)
            if partition_field:
                table.time_partitioning = bigquery.TimePartitioning(type_=bigquery.TimePartitioningType.YEAR,
                                                                    field=partition_field)
            table = self.Client_Bq.create_table(table)
            if table:
                return True
        except exceptions.Conflict:
            msg  = f"Table {table_id} already exists in dataset {dataset_id} "
            msg += f"for project {self.Project_Id}"
            print(msg)
        except exceptions.NotFound:
            msg = f"Dataset {dataset_id} not found for project {self.Project_Id}"
            print(msg)
        except Exception as e:
            print(e)
        return None
    
    def get_dataset(self, dataset_id, print_exception=False):
        if self.Client_Bq is None:
            print("get_dataset: Client_Bq not set")
            return None
        elif self.Project_Id is None:
            print("get_dataset: Project_Id not set")
            return None
        elif dataset_id is None:
            print("get_dataset: dataset_id not set")
            return None
        try:
            return self.Client_Bq.get_dataset(f"{self.Project_Id}.{dataset_id}")
        except exceptions.NotFound:
            if print_exception:
                print(f"Dataset {dataset_id} was not found in project {self.Project_Id}")
            return None

    def get_datasets(self):
        if self.Client_Bq is None:
            print("get_datasets: Client_Bq not set")
            return None
        elif self.Project_Id is None:
            print("get_datasets: Project_Id not set")
            return None
        try:
            datasets = list(self.Client_Bq.list_datasets())
            if datasets:
                return datasets
        except Exception as e:
            print(e)
        return None

    def get_table(self, dataset_id, table_id):
        if self.Client_Bq is None:
            print("get_table: Client_Bq not set")
            return None
        elif self.Project_Id is None:
            print("get_table: Project_Id not set")
            return None
        elif dataset_id is None:
            print("get_table: dataset_id not set")
            return None
        elif table_id is None:
            print("get_table: table_id not set")
            return None
        try:
            return self.Client_Bq.get_table(f"{self.Project_Id}.{dataset_id}.{table_id}")
        except exceptions.NotFound as e:
            msg = f"Table {table_id} was not found in {dataset_id} project {self.Project_Id}"
            print(msg)
        except Exception as e:
            print(e)
        return None

    def table_insert_data_streaming(self, bq_table, dataset_id, rows, table_field_list):
        if self.Client_Bq is None:
            print("table_insert_data_streaming: Client_Bq not set")
            return None
        elif self.Project_Id is None:
            print("table_insert_data_streaming: Project_Id not set")
            return None
        elif bq_table is None:
            print("table_insert_data_streaming: bq_table not set")
            return None
        elif dataset_id is None:
            print("table_insert_data_streaming: dataset_id not set")
            return None
        elif rows is None:
            print("table_insert_data_streaming: rows not set")
            return None
        elif type(rows) is not list:
            print("table_insert_data_streaming: rows not a list")
            return None
        elif len(rows) < 1:
            print("table_insert_data_streaming: rows are empty")
            return None
        elif table_field_list is None:
            print("table_insert_data_streaming: table_field_list not set")
            return None
        elif type(table_field_list) is not list:
            print("table_insert_data_streaming: table_field_list not a list" )
            return None
        if len(table_field_list) != len(rows[0]):
            msg  = "table_insert_data_streaming: Number of elements in row is different from number of "
            msg += "columns in table_field_list"
            print(msg)
            return None
        try:
            row_insert = []
            for row in rows:
                new_tuple = () 
                for idx, data in enumerate(row):
                    new_data = None
                    if 'bool' in table_field_list[idx]['type'] or table_field_list[idx]['type'] == 'bit':
                        if str(data) == '1' or str(data) == 'true':
                            new_data = True
                        else:
                            new_data = False
                    else:
                        new_data = data
                    new_tuple += (new_data,)
                row_insert.append(new_tuple)
            errors = self.Client_Bq.insert_rows(bq_table, row_insert)
            if errors == []:
                return True
            else:
                print(errors)
                return False
        except Exception as e:
            print(e)
        return None
    
    def table_insert_from_table_without_duplicates(self, bq_table_from_id, bq_table_to_id, bq_table_to_fields, dataset_id, rows):
        if self.Client_Bq is None:
            print("table_insert_from_table_without_duplicates: Client_Bq not set")
            return None
        elif self.Project_Id is None:
            print("table_insert_from_table_without_duplicates: Project_Id not set")
            return None
        elif bq_table_from_id is None:
            print("table_insert_from_table_without_duplicates: bq_table_from_id not set")
            return None
        elif bq_table_to_id is None:
            print("table_insert_from_table_without_duplicates: bq_table_to_id not set")
            return None
        elif bq_table_to_fields is None:
            print("table_insert_from_table_without_duplicates: bq_table_to_fields not set")
            return None
        elif dataset_id is None:
            print("table_insert_from_table_without_duplicates: dataset_id not set")
            return None
        elif rows is None:
            return None
        query  = f"""INSERT INTO  ("""
        for field in bq_table_from_id:
            query += f"""{field}, """
        query += f"""date_trusted) """ 
        query += f"""SELECT tb.*, CURRENT_DATETIME('-03:00') AS date_trusted
                    FROM ( SELECT * EXCEPT(rank)
                        FROM ( SELECT *, DENSE_RANK() OVER (PARTITION BY id_big_int ORDER BY datetime_teste DESC) rank
                            FROM `{self.Project_Id}.{dataset_id}.{bq_table_from_id}` as tb)
                    WHERE
                    rank = 1 ) tb"""
        try:
            rows = self.Client_Bq.query_and_wait(query)
        except Exception as e:
            print(e)
        return None
    
    def table_select_data(self, query):
        if self.Client_Bq is None:
            print("get_table: Client_Bq not set")
            return None
        elif self.Project_Id is None:
            print("get_table: Project_Id not set")
            return None
        elif query is None:
            print("get_table: query not set")
            return None
        try:
            rows = self.Client_Bq.query_and_wait(query)
            return rows
        except Exception as e:
            print(e)
        return None
    
    def table_truncate_data(self, dataset_id, table_id):
        if self.Client_Bq is None:
            print("get_table: Client_Bq not set")
            return None
        elif self.Project_Id is None:
            print("get_table: Project_Id not set")
            return None
        elif dataset_id is None:
            print("get_table: dataset_id not set")
            return None
        elif table_id is None:
            print("get_table: table_id not set")
            return None
        query = f"""TRUNCATE TABLE `{self.Project_Id}.{dataset_id}.{table_id}` """
        try:
            rows = self.Client_Bq.query_and_wait(query)
            return rows
        except Exception as e:
            print(e)
        return None