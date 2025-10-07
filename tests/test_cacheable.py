try:
    import pytest
    import pandas as pd
except ImportError:
    print("Pytest not installed - install with `poetry install --with tests`")
    raise

from dataclass_wizard import JSONWizard

import json
from pathlib import Path
from time import sleep
from tqdm import tqdm
from dataclasses import dataclass
import shutil

import toml

from cacheable.cache import Cacheable
from cacheable.params import AbstractParams

from dotenv import dotenv_values

cache_folder = dotenv_values(".env")["CACHE_FOLDER"]

# clean the cache folder
if Path(cache_folder).exists():
    shutil.rmtree(cache_folder)


@dataclass
class Params(JSONWizard):
    """Mock params for testing purposes."""

    run_tag: str = "tag"

    A1: str = "A1 param"
    A2: str = "A2 param"
    A3: str = "A3 param"
    B1: str = "B1 param"
    C1: str = "C1 param"
    C2: str = "C2 param"


class A(Cacheable):

    def __init__(self, A1, A2, A3, run_tag=""):
        super().__init__(run_tag=run_tag)
        self.A1 = A1
        self.A2 = A2
        self.A3 = A3

    def create(self):
        for _i in tqdm(range(10)):
            sleep(0.1)
        return f"I am the object A with params {self.A1}, {self.A2}, {self.A3}"

    @classmethod
    def load_from_file(cls, path):
        file = path / "A.txt"
        with open(file) as f:
            return f.read()

    def save_to_file(self, path):
        file = path / "A.txt"
        with open(file, "w") as f:
            f.write(self.obj)


class B(Cacheable):

    def __init__(self, A3, run_tag="", B1="default B1"):
        super().__init__(run_tag=run_tag)
        self.A3 = A3
        self.B1 = B1

    def create(self):
        for _i in tqdm(range(100)):
            sleep(0.1)
        return pd.DataFrame({"A3": [self.A3], "B1": [self.B1]})

    @classmethod
    def load_from_file(cls, path):
        file = path / "B.csv"
        return pd.read_csv(file)

    def save_to_file(self, path):
        file = path / "B.csv"
        self.obj.to_csv(file)


class C(Cacheable):

    def __init__(self, A, B, run_tag="", **kwargs):
        super().__init__(run_tag=run_tag)
        self.A = A
        self.B = B
        self.kwargs = kwargs

    def create(self):
        for _i in tqdm(range(100)):
            sleep(0.1)
        return {"A": self.A, "B": self.B, **(self.kwargs)}

    @classmethod
    def load_from_file(cls, path):
        file = path / "C.csv"
        with open(file) as f:
            return json.load(f)

    def save_to_file(self, path):
        file = path / "C.json"
        serial = self.kwargs.copy()
        serial["A"] = str(self.obj["A"])
        serial["B"] = str(self.obj["B"])
        with open(file, "w") as f:
            json.dump(serial, f)


def test_cacheable_basic_compute():
    """Test basic computation of cacheable objects A, B, and C."""
    params = Params()

    A_obj = A(params.A1, params.A2, params.A3).compute()
    B_obj = B(params.B1, A_obj).compute()
    C_obj = C(A_obj, B_obj, C1=params.C1, C2=params.C2).compute()

    assert A_obj == f"I am the object A with params {params.A1}, {params.A2}, {params.A3}"
    assert isinstance(B_obj, pd.DataFrame)
    assert isinstance(C_obj, dict)
    assert C_obj["A"] == A_obj


def test_cacheable_load_with_different_tag():
    """Test that changing run_tag loads from cache instead of creating new folder."""
    params = Params()

    # First compute
    A(params.A1, params.A2, params.A3, run_tag="first").compute()

    # Change tag - should load from cache, not create new folder
    A_cache = A(params.A1, params.A2, params.A3, run_tag="second")

    assert "second" not in str(A_cache.cache_folder)


def test_cacheable_load_with_empty_tag():
    """Test that empty run_tag also loads from cache."""
    params = Params()

    # First compute
    A(params.A1, params.A2, params.A3, run_tag="tag").compute()

    # Empty tag - should also load
    A_obj = A(params.A1, params.A2, params.A3, run_tag="").compute()

    assert A_obj == f"I am the object A with params {params.A1}, {params.A2}, {params.A3}"


def test_cacheable_new_folder_on_param_change():
    """Test that changing a parameter creates a new folder with empty tag."""
    params = Params()

    # First compute
    A_cache = A(params.A1, params.A2, params.A3)
    A_obj = A_cache.compute()
    old_cache_folder = A_cache.cache_folder

    # Change parameter - should create new folder with empty tag
    params.A1 = "new A1"
    A_cache = A(params.A1, params.A2, params.A3)
    A_obj = A_cache.compute()

    assert A_obj == f"I am the object A with params {params.A1}, {params.A2}, {params.A3}"

    new_cache_folder = A_cache.cache_folder
    assert new_cache_folder != old_cache_folder


def test_register_save_and_load():
    """Test the register function."""
    params = Params()

    filename = "./tests/A_test.toml"

    A_cache = A(params.A1, params.A2, params.A3)
    A_obj = A_cache.compute()
    A_cache.register(filename, comment="This is a registered A object")

    assert Path(filename).exists()

    # read the file and check contents
    with open(filename) as f:
        data_dict = toml.load(f)
        assert data_dict["comment"] == "This is a registered A object"
        assert "git_commit" in data_dict.keys()
        assert "git_repo" in data_dict.keys()

    # load the file and check contents
    A_obj_loaded = A.load_from_register(filename)
    assert A_obj_loaded == A_obj
