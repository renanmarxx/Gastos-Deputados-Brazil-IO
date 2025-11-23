import csv
import gzip
import io
import json
import os
import shutil
from urllib.parse import urlencode, urljoin
from urllib.request import Request, urlopen

a+b

class BrasilIO:

    base_url = "https://api.brasil.io/v1/"

    def __init__(self, auth_token):
        self.__auth_token = auth_token

    @property
    def headers(self):
        return {
            "User-Agent": "python-urllib/brasilio-client-0.1.0",
        }
        
    @property
    def api_headers(self):
        data = self.headers
        data.update({"Authorization": f"Token {self.__auth_token}"})
        return data

    def api_request(self, path, query_string=None):
        url = urljoin(self.base_url, path)
        if query_string:
            url += "?" + urlencode(query_string)
        request = Request(url, headers=self.api_headers)
        response = urlopen(request)
        return json.load(response)

    def data(self, dataset_slug, table_name, filters=None):
        url = f"dataset/{dataset_slug}/{table_name}/data/"
        filters = filters or {}
        filters["page"] = 1

        finished = False
        while not finished:
            response = self.request(url, filters)
            next_page = response.get("next", None)
            for row in response["results"]:
                yield row
            filters = {}
            url = next_page
            finished = next_page is None

    def download(self, dataset, table_name):
        url = f"https://data.brasil.io/dataset/{dataset}/{table_name}.csv.gz"
        request = Request(url, headers=self.headers)
        response = urlopen(request)
        return response


if __name__ == "__main__":
    api = BrasilIO("meu-api-token")
    dataset_slug = "gastos-deputados"
    table_name = "cota_parlamentar"

    # To download the full file:
    # After downloading, it will store the file on local memory in `data/` folder. 
    
    # Connects to the API:
    response = api.download(dataset_slug, table_name)
    
    # Check if `data/` folder exists, if so cleans the entire directory. Otherwise, it will create a new folder:
    if os.path.exists("data"):
        shutil.rmtree("data")
    os.makedirs("data", exist_ok=True)

    # Defining file path:
    out_path = os.path.join("data", f"{dataset_slug}_{table_name}.csv.gz")

    # Defining chunks to store the file to avoid memory overloads:
    chunk_size = 16 * 1024
    
    # Writing the file in chunks:
    with open(out_path, mode="wb") as fobj:
        while True:
            chunk = response.read(chunk_size)
            if not chunk:
                break
            fobj.write(chunk)

    print(f"File stored succesfuly at: {out_path}")
    