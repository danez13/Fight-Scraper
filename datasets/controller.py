from .dataset import Dataset
from typing import Callable

class DataController():
    def __init__(self,datasets:list[str],update:bool,direct:bool):
        self.datasets = {}
        for dataset in datasets:
            self.datasets[dataset] = Dataset(dataset,update)
        self.direct = direct
    def insert(self,dataset:str,data:dict[str,str]|list[dict[str,str]],prepend:bool=False):
        if dataset not in self.datasets:
            raise TypeError(f"no dataset {dataset}")
        if isinstance(data,dict):
            self.datasets[dataset].add_row(data,prepend)
        else:
            self.datasets[dataset].add_rows(data,prepend)
        self.save(dataset,self.direct)
        return True
    def drop(self,dataset:str,column:str|list):
        if dataset not in self.datasets:
            raise TypeError(f"no dataset {dataset}")
        if isinstance(column,str):
            if column not in self.datasets[dataset]:
                raise TypeError(f"dataset {dataset} does not contain {column}")
            del self.datasets[dataset][column]
        else:
            for col in column:
                if col not in self.datasets[dataset]:
                    raise TypeError(f"dataset {dataset} does not contain {column}")
                del self.datasets[dataset][col]
        self.save(dataset,self.direct)

    def select(self,dataset:str,key:str|list[str]):
        if dataset not in self.datasets:
            raise TypeError(f"no dataset {dataset}")
        return self.datasets[dataset][key]

    def get_early_stopping(self,dataset:str)->Callable:
        if dataset not in self.datasets:
            raise TypeError(f"no dataset {dataset}")
        return self.datasets[dataset].does_id_exist
    
    def save(self,dataset:str,direct:bool):
        if dataset not in self.datasets:
            raise TypeError(f"no dataset {dataset}")
        
        self.datasets[dataset].save(direct)