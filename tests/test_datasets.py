import pytest
from datasets import Dataset  # Your real Dataset class
from datasets import Dataset as RealDataset  # adjust as needed
import datasets  # module where Dataset is used (replace 'yourmodule')

@pytest.fixture(autouse=True)
def patch_dataset(monkeypatch):
    # Replace Dataset with MockDataset everywhere inside your module
    monkeypatch.setattr(datasets, "Dataset", MockDataset)


class MockDataset:
    def __init__(self, name="", columns=None):
        self.name = name
        self.columns = columns or ["id"]
        self.data = []
        self.saved = False
        self.saved_direct = None

    def does_id_exist(self, id):
        return any(row.get("id") == id for row in self.data)

    def add_row(self, row):
        if not isinstance(row, dict):
            raise ValueError("Row must be a dictionary.")
        self.data.append(row)

    def update_row(self, id, new_row):
        for i, row in enumerate(self.data):
            if row.get("id") == id:
                self.data[i] = {**row, **new_row}
                return
        raise ValueError(f"ID {id} does not exist in the dataset.")

    def save(self, direct=False):
        self.saved = True
        self.saved_direct = direct
def test_does_id_exist():
    ds = MockDataset()
    ds.add_row({"id": "1", "name": "Alice"})
    assert ds.does_id_exist("1") is True
    assert ds.does_id_exist("999") is False


def test_add_row_and_update_row():
    ds = MockDataset()
    ds.add_row({"id": "1", "name": "Alice"})
    assert ds.data[0]["name"] == "Alice"

    ds.update_row("1", {"name": "Bob"})
    assert ds.data[0]["name"] == "Bob"

    with pytest.raises(ValueError):
        ds.update_row("999", {"name": "Nope"})


def test_add_row_invalid_type():
    ds = MockDataset()
    with pytest.raises(ValueError):
        ds.add_row(["not", "a", "dict"])


def test_save_called():
    ds = MockDataset()
    ds.add_row({"id": "1"})
    ds.save(direct=True)
    assert ds.saved is True
    assert ds.saved_direct is True

    ds.save(direct=False)
    assert ds.saved_direct is False
