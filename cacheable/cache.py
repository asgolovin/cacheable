import hashlib
import json
import logging
import os
import re
import shutil
from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from time import sleep
from typing import Any

import pandas as pd
from dotenv import dotenv_values
from tqdm import tqdm

from .params import params_from_json, params_to_json


class Cacheable(ABC):
    """
    A magic class to cache expensive objects based on their input arguments.

    If the object with exactly the same parameters already exists, it is loaded from cache, if not,
    a new object is created and saved.

    A Cacheable object can depend on two types of input arguments: other Cacheable objects and
    parameters. To check if the object is already in the cache, we hash the parameters and the
    parameters that other Cacheable objects depend on. If the hash is the same, the object is
    considered to be the same. The reason why we do not hash the objects themselves is that this
    could be a very expensive operation.

    This is an abstract class; the subclasses need to implement the create, load, and save methods.
    """

    depends_on_obj = ()
    depends_on_params = ()
    name = ""

    def __init__(self, params):
        self.params = params
        _ = self.cache_folder  # trigger the creation of the cache folder
        self.logger = logging.getLogger("cache")

    def compute(self, *args):
        cache_folder = self.cache_folder
        logger = self.logger

        # Try to load the object from cache
        if cache_folder.exists():
            logger.info(f"Trying to load {self.name} from cache...")
            try:
                self.obj = self.load_from_file(cache_folder)
                logger.info(f"✔️ Loaded {self.name} from {cache_folder}")
                return self.obj
            except FileNotFoundError:
                logger.info(f"Failed to load {self.name}")
                pass

        # If the object is not in the cache, create it
        logger.info(f"Creating {self.name} from scratch...")
        # select the params that the object depends on
        kwargs = {
            param_name: getattr(self.params, param_name) for param_name in self.depends_on_params
        }
        self.obj: Any = self.create(*args, **kwargs)
        logger.info(f"✔️ Successfully created {self.name}")
        self.save()
        logger.info(f"Saved {self.name} to {cache_folder}")
        return self.obj

    @property
    def cache_folder(self):
        config = {
            **dotenv_values("example.env"),
            **dotenv_values(".env"),
        }
        cache_folder = os.environ.get("CACHE_FOLDER", config.get("CACHE_FOLDER"))
        if cache_folder is None:
            raise ValueError(
                "The CACHE_FOLDER environment variable is not set. Add it to your .env file."
            )
        cache_folder = Path(cache_folder)

        object_folder = cache_folder / self.name
        # If the cache folder already exists (even if with a different tag), return it
        existing_folder = self.find_cache_folder(object_folder)
        if existing_folder:
            return existing_folder

        # If not, create a new folder. The folder has the following structure:
        # <class name>_<run_tag>_<hash>
        name_components = [self.name]
        if len(self.params.run_tag) > 0:
            name_components.append(self.params.run_tag)
        name_components.append(self.hash(self.params))

        folder_name = "_".join(name_components)

        path = object_folder / folder_name
        path.mkdir(parents=True, exist_ok=True)

        # save the params themselves to the cache folder
        param_cache_path = path / "params.json"
        if not param_cache_path.exists():
            params_to_json(self.params, param_cache_path)

        return path

    @classmethod
    def hash(cls, params):
        """Compute a hash of the parameters that the object depends on."""
        hashstr = []
        for parent_cls in cls.depends_on_obj:
            hashstr.append(f"{parent_cls}: {parent_cls.hash(params)}")
        for param_name in cls.depends_on_params:
            param_value = getattr(params, param_name)
            param_hash = hashlib.sha1(str(param_value).encode("UTF-8")).hexdigest()
            hashstr.append(f"{param_name}: {param_hash}")
        hashstr = "\n".join(hashstr)
        hash = hashlib.sha1(hashstr.encode("UTF-8")).hexdigest()
        return hash

    def find_cache_folder(self, object_folder):
        """
        Try to find a folder <class_name>_<run_tag>_<hash>, where the class name and the hash
        must match, but the tag can be anything.
        """
        if not object_folder.exists():
            return False

        pattern = rf"{self.name}_(.*_)?{self.hash(self.params)}"
        for folder in object_folder.iterdir():
            if re.match(pattern, folder.name):
                return folder
        return False

    @classmethod
    def fix_cache_folder(cls, old_folder):
        """
        A band-aid function in case I fucked up and need to update the hash in the name of the cache folder.
        Load the object from the old folder, compute the hash of the object and rename the folder.
        """
        old_folder = Path(old_folder)

        old_folder_name = old_folder.name

        params_folder = old_folder / "params.json"
        if not params_folder.exists():
            raise FileNotFoundError(f"File {params_folder} does not exist")

        params = params_from_json(params_folder)

        old_hash = old_folder_name.split("_")[-1]
        new_hash = cls.hash(params)

        if old_hash == new_hash:
            print("Hashes are the same, nothing to do")
            return

        old_components = old_folder_name.split("_")[:-1]
        new_folder_name = "_".join(old_components + [new_hash])

        shutil.move(old_folder, old_folder.parent / new_folder_name)
        print(f"Renamed {old_folder_name} to {new_folder_name}")

    @abstractmethod
    def create(self, *args, **kwargs) -> Any:
        """Create a new object from scratch, without loading from cache"""
        pass

    def load(self) -> Any:
        path = self.cache_folder
        try:
            self.obj = self.load_from_file(path)
            return self.obj
        except FileNotFoundError as e:
            raise FileNotFoundError(
                f"File {path} does not exist. Please run compute() first."
            ) from e

    @classmethod
    @abstractmethod
    def load_from_file(cls, path) -> Any:
        """Load the object from cache"""
        pass

    @abstractmethod
    def save(self) -> None:
        """Save the object to cache"""
        pass
