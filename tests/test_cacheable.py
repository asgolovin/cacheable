try:
    import pytest
    import pandas as pd
except ImportError:
    print("Pytest not installed - install with `poetry install --with tests`")
    raise

import json
from pathlib import Path
from time import sleep
from tqdm import tqdm
from dataclasses import dataclass
import shutil

from cacheable.cache import Cacheable
from cacheable.params import AbstractParams

from dotenv import dotenv_values

cache_folder = dotenv_values(".env")["CACHE_FOLDER"]

# clean the cache folder
if Path(cache_folder).exists():
    shutil.rmtree(cache_folder)


@dataclass
class Params(AbstractParams):
    """Mock params for testing purposes."""

    A1 = "A1 param"
    A2 = "A2 param"
    A3 = "A3 param"
    B1 = "B1 param"
    C1 = "C1 param"
    C2 = "C2 param"


class A(Cacheable):
    depends_on_obj = ()
    depends_on_params = ("A1", "A2", "A3")
    name = "A"

    def create(self, A1, A2, A3):
        for _i in tqdm(range(100)):
            sleep(0.1)
        return f"I am the object A with params {A1}, {A2}, {A3}"

    @classmethod
    def load_from_file(cls, path):
        file = path / "A.txt"
        with open(file) as f:
            return f.read()

    def save(self):
        file = self.cache_folder / "A.txt"
        with open(file, "w") as f:
            f.write(self.obj)


class B(Cacheable):
    depends_on_obj = ()
    depends_on_params = ("A3", "B1")
    name = "B"

    def create(self, A3, B1):
        for _i in tqdm(range(100)):
            sleep(0.1)
        return pd.DataFrame({"A3": [A3], "B1": [B1]})

    @classmethod
    def load_from_file(cls, path):
        file = path / "B.csv"
        return pd.read_csv(file)

    def save(self):
        file = self.cache_folder / "B.csv"
        self.obj.to_csv(file)


class C(Cacheable):
    depends_on_obj = (A, B)
    depends_on_params = ("C1", "C2")
    name = "C"

    def create(self, A, B, C1, C2):
        for _i in tqdm(range(100)):
            sleep(0.1)
        return {"A": A, "B": B, "C1": C1, "C2": C2}

    @classmethod
    def load_from_file(cls, path):
        file = path / "C.csv"
        with open(file) as f:
            return json.load(f)

    def save(self):
        file = self.cache_folder / "C.csv"
        serial = {"C1": self.obj["C1"], "C2": self.obj["C2"]}
        serial["A"] = str(self.obj["A"])
        serial["B"] = str(self.obj["B"])
        with open(file, "w") as f:
            json.dump(serial, f)


def test_cacheable_basic_compute():
    """Test basic computation of cacheable objects A, B, and C."""
    params = Params()

    A_obj = A(params).compute()
    B_obj = B(params).compute()
    C_obj = C(params).compute(A_obj, B_obj)

    assert A_obj == f"I am the object A with params {params.A1}, {params.A2}, {params.A3}"
    assert isinstance(B_obj, pd.DataFrame)
    assert isinstance(C_obj, dict)
    assert C_obj["A"] == A_obj


def test_cacheable_load_with_different_tag():
    """Test that changing run_tag loads from cache instead of creating new folder."""
    params = Params()

    # First compute
    A(params).compute()

    # Change tag - should load from cache, not create new folder
    params.run_tag = "second"
    A_cache = A(params)

    assert "second" not in str(A_cache.cache_folder)


def test_cacheable_load_with_empty_tag():
    """Test that empty run_tag also loads from cache."""
    params = Params()

    # First compute
    A(params).compute()

    # Empty tag - should also load
    params.run_tag = ""
    A_obj = A(params).compute()

    assert A_obj == f"I am the object A with params {params.A1}, {params.A2}, {params.A3}"


def test_cacheable_new_folder_on_param_change():
    """Test that changing a parameter creates a new folder with empty tag."""
    params = Params()

    # First compute
    A_cache = A(params)
    A_obj = A_cache.compute()
    old_cache_folder = A_cache.cache_folder

    # Change parameter - should create new folder with empty tag
    params.A1 = "new A1"
    A_cache = A(params)
    A_obj = A_cache.compute()

    assert A_obj == f"I am the object A with params {params.A1}, {params.A2}, {params.A3}"

    new_cache_folder = A_cache.cache_folder
    assert new_cache_folder != old_cache_folder
