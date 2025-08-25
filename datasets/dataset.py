import pandas as pd
import os
import logging
import tempfile
logger = logging.getLogger(__name__)
class Dataset():
    def __init__(self, file: str, update:bool, columns: list = ["id"],disabled:bool=False):
        self.disabled = disabled
        if not disabled:
            self.file = file
            self.update = update


            if os.path.exists(self.file+".csv"):
                logger.debug(f"Loading data from {self.file}")
                self.data = pd.read_csv(self.file+".csv")
                self.columns = self.data.columns.tolist()
            else:
                self.columns = columns
                logger.info(f"File {self.file} does not exist. Initializing new DataFrame.")
                self.data = pd.DataFrame(columns=self.columns)

            self.tmp_file = tempfile.NamedTemporaryFile(
                    mode='w+', 
                    suffix='.csv', 
                    prefix=f'{self.file}_progress_', 
                    dir="",
                    delete=False
                )
        
    def __getitem__(self,key:str):
        if not self.disabled:
            return self.data[key]
        else:
            raise TypeError("dataset is disabled")

    def does_id_exist(self, id: str) -> bool:
        """Check if an ID exists in the dataset."""

        if not self.disabled:
            if 'id' not in self.data.columns:
                raise ValueError("Dataset does not contain 'id' column.")
                    
            if self.update:
                return False
            return id in self.data['id'].values
        else:
            raise TypeError("dataset is disabled")
    
    def add_row(self, row:dict,prepend:bool=False):
        """Add a new row to the dataset."""

        if not self.disabled:
            if not isinstance(row, dict):
                raise ValueError("Row must be a dictionary.")
            
            new_data = pd.DataFrame([row])

            if self.data.empty:
                self.data = new_data
            elif prepend:
                self.data = pd.concat([new_data, self.data], ignore_index=True)
            else:
                self.data = pd.concat([self.data, new_data], ignore_index=True)
        else:
            raise TypeError("dataset is disabled")

    def add_rows(self,rows:list[dict],prepend:bool=False):
        if not self.disabled:
            if not isinstance(rows,list):
                raise ValueError("Rows must be a list")
            
            for row in rows:
                self.add_row(row,prepend=prepend)
        else: 
            raise TypeError("dataset is disabled")


    def update_row(self, id: str, row: dict):
        """Update an existing row in the dataset."""

        if not self.disabled:
            if 'id' not in self.data.columns:
                raise ValueError("Dataset does not contain 'id' column.")
            
            if id not in self.data['id'].values:
                raise ValueError(f"ID {id} does not exist in the dataset.")
            
            # Ensure only valid columns are updated
            invalid_keys = set(row.keys()) - set(self.data.columns)
            if invalid_keys:
                raise ValueError(f"Invalid column(s): {invalid_keys}")
            
            self.data.loc[self.data['id'] == id, list(row.keys())] = list(row.values())
        else:
            raise TypeError("dataset is disabled")



    def update_rows(self, ids: list[str], rows: list[dict]):
        if not self.disabled:
            """Update multiple rows in the dataset."""
            for id, row in zip(ids, rows):
                self.update_row(id, row)
        else:
            raise TypeError("dataset is disabled")


    def save(self,direct:bool):
        """Save the dataset to the CSV file."""
        if not self.disabled:

            if direct:
                if not self.file:
                    raise ValueError("File path is not set.")

                logger.debug(f"Saving dataset to {self.file}")
                if os.path.exists(self.tmp_file.name):
                    try:
                        self.data = pd.read_csv(self.tmp_file.name)
                    except:
                        pass
                    self.tmp_file.close()
                    os.remove(self.tmp_file.name)

                self.data.to_csv(self.file+".csv", index=False)

            else:
                if not self.tmp_file:
                    raise ValueError("Temporary file is not set.")

                self.data.to_csv(self.tmp_file.name, index=False)
                logger.debug(f"Saving dataset to temporary file {self.tmp_file.name}")
        else:
            raise TypeError("dataset is disabled")