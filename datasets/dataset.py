import pandas as pd
import os
import logging
import tempfile

logger = logging.getLogger(__name__)

class Dataset:
    def __init__(self, file: str, columns: list = ["id"]):
        self.file = file
<<<<<<< HEAD

        if os.path.exists(self.file + ".csv"):
            logger.debug(f"Loading data from {self.file}")
            self.data = pd.read_csv(self.file + ".csv", dtype={"id": str})
            self.columns = self.data.columns.tolist()
            
=======
        self.columns = columns
        self.data = pd.read_csv(self.file+ ".csv")

        if os.path.exists(self.file+".csv"):
            logger.debug(f"Loading data from {self.file}")
            self.data = pd.read_csv(self.file+".csv")
>>>>>>> parent of a5be162 (Refactor Dataset and UFCStatsScraper: Improve column handling and clean text method)
        else:
            logger.info(f"File {self.file} does not exist. Initializing new DataFrame.")
            self.data = pd.DataFrame(columns=self.columns)

        # In-memory buffer for staged changes
        self.buffer = pd.DataFrame(columns=self.columns)

        # Temporary file for progress saves
        self.tmp_file = tempfile.NamedTemporaryFile(
            mode='w+',
            suffix='.csv',
            prefix=f'{self.file}_progress_',
            dir="./",
            delete=False
        )

    def get_column(self, from_buffer:bool=True, column:str="id") -> list[str]:
        target_df = self.buffer if from_buffer else self.data
        if column not in target_df.columns:
            raise ValueError(f"Dataset does not contain {column} column.")
        
        return target_df[column].tolist()
    
    def get_instance_column(self,id:str, column:str, from_buffer:bool=True, ):
        target_df = self.buffer if from_buffer else self.data
        if column not in target_df.columns:
            raise ValueError(f"Dataset does not contain {column} column.")
        return target_df.loc[target_df["id"] == id, column].tolist()[0]

    def does_id_exist(self, id: str, to_buffer: bool = True) -> bool:
        """
        Check if an ID exists in either the buffer or main dataset.
        to_buffer=True → check buffer
        to_buffer=False → check main dataset
        """
        if 'id' not in self.columns:
            raise ValueError("Dataset does not contain 'id' column.")
        
        return id in (self.buffer if to_buffer else self.data).values

    def add_row(self, row: dict, to_buffer: bool = True):
        """
        Add a new row to buffer (default) or main dataset.
        """
        if not isinstance(row, dict):
            raise ValueError("Row must be a dictionary.")
        
        if to_buffer:
            self.buffer = pd.concat([self.buffer, pd.DataFrame([row])], ignore_index=True)
        else:
            self.data = pd.concat([self.data, pd.DataFrame([row])], ignore_index=True)

    def update_row(self, id: str, row: dict, to_buffer: bool = True):
        """
        Update an existing row in buffer (default) or main dataset.
        """
        target_df = self.buffer if to_buffer else self.data

        if 'id' not in target_df.columns:
            raise ValueError("Dataset does not contain 'id' column.")
        
        if id not in target_df['id'].values:
            raise ValueError(f"ID {id} does not exist in the {'buffer' if to_buffer else 'dataset'}.")

        target_df.loc[target_df['id'] == id, list(row.keys())] = list(row.values())

        if to_buffer:
            self.buffer = target_df
        else:
            self.data = target_df

    def flush(self):
        """Merge buffer into main dataset (overwrite duplicates by ID) and clear buffer."""
        if self.buffer.empty:
            return
        combined = pd.concat([self.data, self.buffer], ignore_index=True)
        combined = combined.drop_duplicates(subset="id", keep="last")
        self.data = combined
        self.buffer = pd.DataFrame(columns=self.columns)

    def save(self, direct: bool = False):
        """
        Save dataset to file.
        direct=False → temp file
        direct=True  → main file
        Always clears buffer after saving.
        """
        # Flush changes before saving
        self.flush()

        if direct:
            logger.debug(f"Saving dataset to final file {self.file}.csv")
            self.data.to_csv(self.file + ".csv", index=False)
            if os.path.exists(self.tmp_file.name):
<<<<<<< HEAD
=======
                self.data = pd.read_csv(self.tmp_file.name)
>>>>>>> parent of a5be162 (Refactor Dataset and UFCStatsScraper: Improve column handling and clean text method)
                self.tmp_file.close()
                os.remove(self.tmp_file.name)
        else:
            logger.debug(f"Saving dataset to temporary file {self.tmp_file.name}")
            self.data.to_csv(self.tmp_file.name, index=False)
