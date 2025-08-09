import os
import pytest
import pandas as pd
from datasets import Dataset  # Adjust import to your actual path


@pytest.fixture
def fresh_dataset(tmp_path):
    file_path = tmp_path / "testdataset"
    ds = Dataset(str(file_path), columns=["id", "col1", "col2"])
    yield ds
    # Cleanup after test
    csv_path = str(file_path) + ".csv"
    if os.path.exists(csv_path):
        os.remove(csv_path)
    # Close and remove tmp_file properly
    try:
        ds.tmp_file.close()  # <- Close the file handle
    except Exception:
        pass
    if os.path.exists(ds.tmp_file.name):
        os.remove(ds.tmp_file.name)



def test_init_creates_empty_dataframe_if_no_file(fresh_dataset):
    ds = fresh_dataset
    assert isinstance(ds.data, pd.DataFrame)
    assert list(ds.data.columns) == ["id", "col1", "col2"]
    assert ds.buffer.empty


def test_init_loads_existing_file(tmp_path):
    file_path = tmp_path / "existing"
    df = pd.DataFrame([{"id": "1", "col1": "a", "col2": "b"}])
    df.to_csv(str(file_path) + ".csv", index=False)

    ds = Dataset(str(file_path), columns=["id", "col1", "col2"])
    assert not ds.data.empty
    assert "1" in ds.data["id"].values


def test_add_row_to_buffer_and_data(fresh_dataset):
    ds = fresh_dataset
    row = {"id": "1", "col1": "val1", "col2": "val2"}

    ds.add_row(row)
    assert not ds.buffer.empty
    assert ds.buffer.iloc[0]["id"] == "1"

    ds.add_row({"id": "2", "col1": "val3", "col2": "val4"}, to_buffer=False)
    assert not ds.data.empty
    assert "2" in ds.data["id"].values


def test_add_row_raises_with_invalid_input(fresh_dataset):
    ds = fresh_dataset
    with pytest.raises(ValueError):
        ds.add_row(["not", "a", "dict"])


def test_does_id_exist_checks_buffer_and_data(fresh_dataset):
    ds = fresh_dataset
    row = {"id": "1", "col1": "val1", "col2": "val2"}
    ds.add_row(row)  # buffer now has id=1
    assert ds.does_id_exist("1", to_buffer=True)
    assert not ds.does_id_exist("1", to_buffer=False)

    # Add to main data
    ds.add_row({"id": "2", "col1": "val3", "col2": "val4"}, to_buffer=False)
    assert ds.does_id_exist("2", to_buffer=False)
    assert not ds.does_id_exist("2", to_buffer=True)


def test_get_column_returns_correct_list(fresh_dataset):
    ds = fresh_dataset
    ds.add_row({"id": "1", "col1": "a", "col2": "b"})
    ds.flush()
    col_data = ds.get_column(from_buffer=False, column="col1")
    assert col_data == ["a"]


def test_get_column_raises_for_missing_column(fresh_dataset):
    ds = fresh_dataset
    with pytest.raises(ValueError):
        ds.get_column(column="missing_col")


def test_get_instance_column_returns_value(fresh_dataset):
    ds = fresh_dataset
    ds.add_row({"id": "id1", "col1": "val1", "col2": "val2"})
    val = ds.get_instance_column("id1", "col1")
    assert val == "val1"


def test_get_instance_column_raises_for_missing_column(fresh_dataset):
    ds = fresh_dataset
    ds.add_row({"id": "id1", "col1": "val1", "col2": "val2"})
    with pytest.raises(ValueError):
        ds.get_instance_column("id1", "missing_col")


def test_get_instance_column_raises_if_id_not_found(fresh_dataset):
    ds = fresh_dataset
    ds.add_row({"id": "id1", "col1": "val1"})
    with pytest.raises(IndexError):
        ds.get_instance_column("not_exist", "col1")


def test_update_row_updates_buffer_and_data(fresh_dataset):
    ds = fresh_dataset
    ds.add_row({"id": "1", "col1": "old", "col2": "x"})
    ds.flush()  # flush moves data from buffer to main dataset and clears buffer
    ds.update_row("1", {"col1": "new"}, to_buffer=False)  # update main dataset
    assert ds.data.loc[ds.data["id"] == "1", "col1"].iloc[0] == "new"

    ds.add_row({"id": "2", "col1": "buffered"}, to_buffer=True)
    ds.update_row("2", {"col2": "updated"}, to_buffer=True)  # update buffer
    assert ds.buffer.loc[ds.buffer["id"] == "2", "col2"].iloc[0] == "updated"



def test_update_row_raises_if_id_missing(fresh_dataset):
    ds = fresh_dataset
    with pytest.raises(ValueError):
        ds.update_row("nonexistent", {"col1": "val"})


def test_flush_merges_buffer_into_data(fresh_dataset):
    ds = fresh_dataset
    ds.add_row({"id": "1", "col1": "val1"})
    ds.flush()
    assert ds.buffer.empty
    assert "1" in ds.data["id"].values

    # Add duplicate ID in buffer, flush overwrites
    ds.add_row({"id": "1", "col1": "val2"})
    ds.flush()
    assert ds.data.loc[ds.data["id"] == "1", "col1"].iloc[0] == "val2"


def test_save_creates_files_and_cleans_buffer(fresh_dataset, tmp_path):
    ds = fresh_dataset
    ds.add_row({"id": "1", "col1": "val1"})
    ds.save(direct=False)

    # Temp file should exist
    assert os.path.exists(ds.tmp_file.name)

    ds.save(direct=True)
    final_file = ds.file + ".csv"
    assert os.path.exists(final_file)

    # Buffer should be empty after save
    assert ds.buffer.empty


def test_save_removes_temp_file_on_direct_save(fresh_dataset):
    ds = fresh_dataset
    ds.add_row({"id": "1", "col1": "val1"})
    ds.save(direct=False)

    tmp_path = ds.tmp_file.name
    assert os.path.exists(tmp_path)

    ds.save(direct=True)
    # Temp file should be removed after direct save
    assert not os.path.exists(tmp_path)


def test_buffer_and_data_independent(fresh_dataset):
    ds = fresh_dataset
    ds.add_row({"id": "1", "col1": "buffered"})
    ds.add_row({"id": "2", "col1": "data"}, to_buffer=False)

    assert "1" in ds.buffer["id"].values
    assert "2" in ds.data["id"].values


def test_columns_remain_consistent_after_operations(fresh_dataset):
    ds = fresh_dataset
    ds.add_row({"id": "1", "col1": "val1"})
    ds.flush()
    ds.update_row("1", {"col1": "val2"}, to_buffer=False)  # Update main data, not buffer
    ds.save(direct=True)

    assert set(ds.data.columns) == set(["id", "col1", "col2"])

