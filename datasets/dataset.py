import pandas as pd
import os
import logging
import tempfile
logger = logging.getLogger(__name__)
class Dataset():
    def __init__(self, file: str, columns: list = ["id"]):
        self.file = file


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
                delete=False
            )

    def does_id_exist(self, id: str) -> bool:
        """Check if an ID exists in the dataset."""
        if 'id' not in self.data.columns:
            raise ValueError("Dataset does not contain 'id' column.")
        return id in self.data['id'].values
    
    def add_row(self, row:dict):
        """Add a new row to the dataset."""
        if not isinstance(row, dict):
            raise ValueError("Row must be a dictionary.")
        
        self.data = pd.concat([self.data, pd.DataFrame([row])], ignore_index=True)

    def update_row(self, id: str, row: dict):
        """Update an existing row in the dataset."""
        if 'id' not in self.data.columns:
            raise ValueError("Dataset does not contain 'id' column.")
        
        if id not in self.data['id'].values:
            raise ValueError(f"ID {id} does not exist in the dataset.")
        
        self.data.loc[self.data['id'] == id, list(row.keys())] = list(row.values())
    
    def save(self,direct:bool = False):
        """Save the dataset to the CSV file."""
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


