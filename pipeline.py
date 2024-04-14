import os

from connector_bigquery import Connector_BigQuery

def get_courses_2018():
    big_query_key_json = os.getenv('GOOGLE_BIG_QUERY_ACCOUNT_SERVICE_JSON_PATH')
    if big_query_key_json is None:
        print("Impossível obter arquivo json de credenciais do Google Big Query")
        return None
    project_id = ''
    dataset_id = 'escola_virtual'
    table_id = 'matricula_2018'
    bq_obj = Connector_BigQuery(big_query_key_json, project_id)
    if bq_obj is None:
        print(f"Não foi possível criar o conector do BigQuery")
        return None
    if(bq_obj.authenticate()) is None:
        print(f"Não foi possivel autenticar no BigQuery")
        return None
    if bq_obj.get_dataset(dataset_id) is None:
        print(f"Dataset {dataset_id} não existe no projeto {bq_obj.Project_Id}")
        return None
    bq_table = bq_obj.get_table(dataset_id, table_id)
    if bq_table is None:
        print(f"Tabela {table_id} não existe no dataset {dataset_id} do projeto {bq_obj.Project_Id}")
        return None
    query = f"""SELECT DISTINCT(nome_curso) FROM `{bq_obj.Project_Id}.{dataset_id}.{table_id}` """
    data = bq_obj.table_select_data(query)
    list_courses_2018 = []
    if data:
        for row in data:
            my_tuple = ()
            my_tuple += (row[0], )
            my_tuple += (2018, )
            list_courses_2018.append(my_tuple)
    if len(list_courses_2018) == 0:
        print("Lista de cursos 2018 não preenchida")
    field_list = []
    field_list.append({ "name": f"cursos", "required":f"True", "type":f"string"})
    field_list.append({ "name": f"ano", "required":f"True", "type":f"int"})
    table_course_2018 = 'cursos_2018'
    bq_table_couse_2018 = bq_obj.get_table(dataset_id, table_id)
    if bq_table_couse_2018 is None:
        if bq_obj.create_table(dataset_id, field_list, table_course_2018) is None:
            print(f"Tabela {table_course_2018} não pode ser criada")
        bq_table_couse_2018 = bq_obj.get_table(dataset_id, table_id)
    if bq_table_couse_2018 is None:
        print(f"Não foi possível obter tabela {table_course_2018} do dataset {dataset_id}")
        return None
    print(bq_table_couse_2018)

get_courses_2018()