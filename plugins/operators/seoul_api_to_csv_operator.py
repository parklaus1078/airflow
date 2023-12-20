from airflow.models.baseoperator import BaseOperator
from airflow.hooks.base import BaseHook
import pandas as pd

class SeoulApiToCsvOperator(BaseOperator):
    template_fields = ("endpoint", "path", "file_name", "base_date")

    def __init__(self, dataset_name, path, file_name, base_date=None, **kwargs):
        super().__init__(**kwargs)
        self.http_conn_id = "seoul_public_data"
        self.path = path
        self.file_name = file_name
        self.endpoint = "{{var.value.apikey_openapi_seoul_go_kr}}/json/" + dataset_name
        self.base_date = base_date

    def execute(self, context):
        import os

        connection = BaseHook.get_connection(self.http_conn_id)
        self.base_url = f"{connection.host}:{connection.port}/{self.endpoint}"
        
        print("self.base_url : " + self.base_url)

        total_row_df = pd.DataFrame()
        start_row = 1
        end_row = 1000
        while True:
            self.log.info(f"시작: {start_row}")
            self.log.info(f"끝: {end_row}")
            row_df = self._call_api(self.base_url, start_row, end_row)
            total_row_df = pd.concat([total_row_df, row_df])
            if len(row_df) < 1000:
                break
            else:
                start_row = end_row + 1
                end_row += 1000

        if not os.path.exists(self.path):
            os.system(f"mkdir -p {self.path}")
        total_row_df.to_csv(self.path + "/" + self.file_name, encoding="utf-8", index=False)

    def _call_api(self, base_url, start_row, end_row):
        import requests
        import json

        headers = {
            "Content-Type": "application/json",
            "charset": "utf-8",
            "Accept": "*/*"
        }

        request_url = f"{base_url}/{start_row}/{end_row}/"
        if self.base_date is not None:
            request_url = f"{base_url}/{start_row}/{end_row}/{self.base_date}"
        
        print("request_url : " + request_url)

        response = requests.get(request_url, headers)
        contents = json.loads(response.text)



        key_name = list(contents.keys())[0]
        row_data = contents.get(key_name).get("row")
        row_df = pd.DataFrame(row_data)

        return row_df
