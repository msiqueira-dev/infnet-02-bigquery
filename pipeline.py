import os

from connector_bigquery import Connector_BigQuery

class Pipeline_Escola_Virtual:

    #Big_Query_Key_Json = GOOGLE_BIG_QUERY_ACCOUNT_SERVICE_JSON_PATH
    #
    # Deve ser variável de ambiente correspondente ao caminho que se encontra o arquivo .json fornecido no IAM 
    # para a conta de serviço, que deve ser criada e permissionada adequadamente ao BigQuery API.
    Big_Query_Key_Json = None
    #Project_id = GOOGLE_BIG_QUERY_PROJECT_ID
    #
    # Deve ser variável de ambiente correspondente ao nome do projeto no BigQuery.
    Project_Id = None
    Dataset_Id = 'escola_virtual'
    Table_Id = None
    Bq_Obj = None

    def get_courses(self, field, table_id, year):
        print(f"Tentando obter {field} da tabela {table_id} para o ano {year} no dataset {self.Dataset_Id}")
        self.Table_Id = table_id
        if self.Project_Id is None:
            self.Project_Id = os.getenv('GOOGLE_BIG_QUERY_PROJECT_ID')
            if self.Project_Id is None:
                print("Não foi possível obter variavel de ambiente GOOGLE_BIG_QUERY_PROJECT_ID")
                return None
        if self.Bq_Obj is None:
            if self.Big_Query_Key_Json is None:
                self.Big_Query_Key_Json = os.getenv('GOOGLE_BIG_QUERY_ACCOUNT_SERVICE_JSON_PATH')
                if self.Big_Query_Key_Json is None:
                    print("Impossível obter arquivo json de credenciais do Google Big Query")
                    return None
            self.Bq_Obj = Connector_BigQuery(self.Big_Query_Key_Json, self.Project_Id)
        if self.Bq_Obj is None:
            print(f"Não foi possível criar o conector do BigQuery")
            return None
        if self.Bq_Obj.Authenticated == False:
            if(self.Bq_Obj.authenticate()) is None:
                print(f"Não foi possivel autenticar no BigQuery")
                return None
        if self.Bq_Obj.get_dataset(self.Dataset_Id) is None:
            print(f"Dataset {self.Dataset_Id} não existe no projeto {self.Bq_Obj.Project_Id}")
            return None
        bq_table = self.Bq_Obj.get_table(self.Dataset_Id, self.Table_Id)
        if bq_table is None:
            print(f"Tabela {self.Table_Id} não existe no dataset {self.Dataset_Id} do projeto {self.Bq_Obj.Project_Id}")
            return None
        query = f"""SELECT DISTINCT({field}) FROM `{self.Bq_Obj.Project_Id}.{self.Dataset_Id}.{self.Table_Id}` """
        data = self.Bq_Obj.table_select_data(query)
        list_courses = []
        if data:
            for row in data:
                my_tuple = ()
                my_tuple += (row[0], )
                my_tuple += (year, )
                list_courses.append(my_tuple)
        if len(list_courses) == 0:
            print(f"Lista de cursos {year} não preenchida")
        else:
            print(f'{len(list_courses)} dados obtidos com sucesso para {self.Table_Id} no dataset {self.Dataset_Id} para o projeto {self.Bq_Obj.Project_Id}')
        return list_courses

    def insert_courses_year(self, list_courses):
        print("insert_courses_year: Preparando inserção de list_courses")
        if list_courses is None:
            print("insert_courses_year: variável list_courses é nula")
            return None
        elif list_courses is type(list):
            print("insert_courses_year: variável list_courses não é do tipo lista")
            return None
        elif len(list_courses) == 0:
            print("insert_courses_year: variável list_courses está vazia")
            return None
        if self.Project_Id is None:
            self.Project_Id = os.getenv('GOOGLE_BIG_QUERY_PROJECT_ID')
            if self.Project_Id is None:
                print("Não foi possível obter variavel de ambiente GOOGLE_BIG_QUERY_PROJECT_ID")
                return None
        if self.Bq_Obj is None:
            if self.Big_Query_Key_Json is None:
                self.Big_Query_Key_Json = os.getenv('GOOGLE_BIG_QUERY_ACCOUNT_SERVICE_JSON_PATH')
                if self.Big_Query_Key_Json is None:
                    print("Impossível obter arquivo json de credenciais do Google Big Query")
                    return None
            self.Bq_Obj = Connector_BigQuery(self.Big_Query_Key_Json, self.Project_Id)
        if self.Bq_Obj is None:
            print(f"Não foi possível criar o conector do BigQuery")
            return None
        if self.Bq_Obj.Authenticated == False:
            if(self.Bq_Obj.authenticate()) is None:
                print(f"Não foi possivel autenticar no BigQuery")
                return None
        self.Table_Id = 'cursos_ano'
        field_list = []
        field_list.append({ "name": f"cursos", "required":f"True", "type":f"string"})
        field_list.append({ "name": f"ano", "required":f"True", "type":f"int"})
        bq_table_couses_year = self.Bq_Obj.get_table(self.Dataset_Id, self.Table_Id)
        if bq_table_couses_year is None:
            print(f'Criando tabela {self.Table_Id} no dataset {self.Dataset_Id} no projeto {self.Bq_Obj.Project_Id}')
            if self.Bq_Obj.create_table(self.Dataset_Id, field_list, self.Table_Id) is None:
                print(f"Tabela {bq_table_couses_year} não pode ser criada no dataset {self.Dataset_Id} no projeto {self.Bq_Obj.Project_Id}")
            else:
                print(f"Tabela {bq_table_couses_year} criada no dataset {self.Dataset_Id} no projeto {self.Bq_Obj.Project_Id}")
            bq_table_couses_year = self.Bq_Obj.get_table(self.Dataset_Id, self.Table_Id)
        if bq_table_couses_year is None:
            print(f"Não foi possível obter tabela {bq_table_couses_year} do dataset {self.Dataset_Id}")
            return None
        print(f"Tentando inserir {len(list_courses)} em {self.Table_Id} no {self.Dataset_Id} no projeto {self.Project_Id}")
        if self.Bq_Obj.table_insert_data_streaming(bq_table_couses_year, self.Dataset_Id, list_courses, field_list):
            print(f"{len(list_courses)} dados inseridos em {self.Table_Id} no {self.Dataset_Id} no projeto {self.Project_Id}")
            return True
        
    def validate_if_courses_year_does_not_exists(self, year):
        if self.Bq_Obj is None:
            if self.Big_Query_Key_Json is None:
                self.Big_Query_Key_Json = os.getenv('GOOGLE_BIG_QUERY_ACCOUNT_SERVICE_JSON_PATH')
                if self.Big_Query_Key_Json is None:
                    print("Impossível obter arquivo json de credenciais do Google Big Query")
                    return None
            self.Bq_Obj = Connector_BigQuery(self.Big_Query_Key_Json, self.Project_Id)
        if self.Bq_Obj is None:
            print(f"Não foi possível criar o conector do BigQuery")
            return None
        if self.Bq_Obj.Authenticated == False:
            if(self.Bq_Obj.authenticate()) is None:
                print(f"Não foi possivel autenticar no BigQuery")
                return None
        self.Table_Id = 'cursos_ano'
        bq_table_couses_year = self.Bq_Obj.get_table(self.Dataset_Id, self.Table_Id)
        if bq_table_couses_year is None:
            return True
        query = f"""SELECT count(1) FROM `{self.Bq_Obj.Project_Id}.{self.Dataset_Id}.{self.Table_Id}` 
                    WHERE ano={year}"""
        data = self.Bq_Obj.table_select_data(query)
        if data is None:
            print(f'Dados de {year} não foram encontrados em {self.Table_Id} no dataset {self.Dataset_Id} no {self.Bq_Obj.Project_Id}')
            return False
        else:
            count = None
            for item in data:
                try:
                    count = item[0]
                except:
                    count = None
            if count == None:
               print(f'Dados de {year} já existem em {self.Table_Id} no dataset {self.Dataset_Id} no {self.Bq_Obj.Project_Id}')
            else:
                if count == 0:
                    print(f'Dados de {year} não foram encontrados em {self.Table_Id} no dataset {self.Dataset_Id} no {self.Bq_Obj.Project_Id}')
                    return False
                print(f'{count} dados para o ano de {year} encontrados em {self.Table_Id} no dataset {self.Dataset_Id} no {self.Bq_Obj.Project_Id}')
            return True
        
        

pipeline_obj = Pipeline_Escola_Virtual()
list_courses_2018 = pipeline_obj.get_courses('nome_curso', 'matricula_2018', 2018)
list_courses_2022 = pipeline_obj.get_courses('nome_curso', 'matricula_2022', 2022)
if pipeline_obj.validate_if_courses_year_does_not_exists(2018) is False:
    pipeline_obj.insert_courses_year(list_courses_2018)
if pipeline_obj.validate_if_courses_year_does_not_exists(2022) is False:
    pipeline_obj.insert_courses_year(list_courses_2022)