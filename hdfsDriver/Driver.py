from hdfs import InsecureClient
import json
import pandas as pd

class Driver:
    def __init__(self, url, user):
        self.client = InsecureClient(url, user=user)
    
    def read(self, path):
        if path.endswith(".json"):
            with self.client.read(path, encoding="utf-8") as reader:
                return json.loads(reader.read())
        if path.endswith(".txt"):
            with self.client.read(path, encoding="utf-8") as reader:
                return reader.read()
        if path.endswith(".csv"):
            with self.client.read(path, encoding="utf-8") as reader:
                return pd.read_csv(reader)
        else:
            raise Exception("file type not support")
        
    def read_from_folder(self, path):
        ls_dir = self.client.list(path)
        data = [] 
        for i in ls_dir:
            data.append(self.read(path+i))
        return pd.DataFrame(data)
    
    def write(self, path, content, file_type):
        if file_type == "json":
            self.client.write(path, json.dumps(content, indent=4), encoding="utf-8", overwrite=True)
        if file_type == "csv":
            with self.client.write(path, encoding="utf-8", overwrite=True) as writer:
                content.to_csv(writer, index=False)
        else:
            raise Exception("file type not support")
        
    def delete(self, path):
        self.client.delete(path)
