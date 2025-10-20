import hashlib
import logging
import os
import re
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any
import toml
import git

import pandas as pd
from dotenv import dotenv_values

import pickle


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

    def __init__(self, run_tag=""):
        self.run_tag = run_tag
        # convert class name to snake_case
        self.name = re.sub(r"(?<!^)(?=[A-Z])", "_", self.__class__.__name__).lower()
        self.logger = logging.getLogger("cache")

    def compute(self):
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
        self.obj: Any = self.create()
        logger.info(f"✔️ Successfully created {self.name}")
        self.save()
        logger.info(f"Saved {self.name} to {cache_folder}")
        return self.obj

    def load(self) -> Any:
        path = self.cache_folder
        try:
            self.obj = self.load_from_file(path)
            return self.obj
        except FileNotFoundError as e:
            raise FileNotFoundError(
                f"File {path} does not exist. Please run compute() first."
            ) from e

    @abstractmethod
    def create(self) -> Any:
        """Create a new object from scratch, without loading from cache"""
        pass

    @classmethod
    @abstractmethod
    def load_from_file(cls, path) -> Any:
        """Load the object from cache"""
        pass

    def save(self) -> None:
        """Save the object to cache"""
        self.cache_folder.mkdir(parents=True, exist_ok=True)
        self.save_to_file(self.cache_folder)

    @abstractmethod
    def save_to_file(self, path) -> None:
        pass

    def register(self, path: str | Path, comment="", save_git_commit=True):
        """
        Create a mapping save_name -> hash of the object so that the object can be easily retrieved
        later without recreating all input arguments.

        Args:
            path (str | Path): a path to a toml file where the mapping will be saved.
            save_name (_type_): _description_
            comment (str, optional): a longer description of the object.
        """

        # check that path has .toml extension
        if not str(path).endswith(".toml"):
            raise ValueError("The path must have a .toml extension")

        metadata = {
            "object_name": self.name,
            "hash": self.hash(),
            "comment": comment,
            "run_tag": self.run_tag,
            "cache_folder": str(self.cache_folder),
            "created_at": str(pd.Timestamp.now()),
            "created_by": os.environ.get("USER", "unknown"),
        }

        if save_git_commit:
            try:
                repo = git.Repo(search_parent_directories=True)
                sha = repo.head.object.hexsha
                metadata["git_commit"] = sha
                metadata["git_repo"] = repo.remotes.origin.url
            except git.exc.InvalidGitRepositoryError:
                self.logger.warning("Not a git repository, skipping git commit hash")

        # save the metadata to a file
        folder = Path(path).parent
        folder.mkdir(parents=True, exist_ok=True)

        with open(path, 'w') as f:
            toml.dump(metadata, f)

    @classmethod
    def load_from_register(cls, filename):
        with open(filename, 'r') as f:
            metadata = toml.load(f)
        cache_folder = Path(metadata['cache_folder'])
        return cls.load_from_file(cache_folder)

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
        if len(self.run_tag) > 0:
            name_components.append(self.run_tag)
        name_components.append(self.hash())

        folder_name = "_".join(name_components)

        path = object_folder / folder_name

        return path

    def hash(self):
        hashstr = []

        attributes = vars(self)

        # exclude keys that should not change the object
        exclude_keys = ["logger", "run_tag", "obj"]

        for key in sorted(attributes.keys()):
            if key in exclude_keys:
                continue
            value = attributes[key]
            if isinstance(value, Cacheable):
                value_hash = value.hash()
            else:
                value_hash = hashlib.sha1(pickle.dumps(value)).hexdigest()
            hashstr.append(f"{key}: {value_hash}")

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

        pattern = rf"{self.name}_(.*_)?{self.hash()}"
        for folder in object_folder.iterdir():
            if re.match(pattern, folder.name):
                return folder
        return False

    # @classmethod
    # def fix_cache_folder(cls, old_folder):
    #     """
    #     A band-aid function in case I fucked up and need to update the hash in the name of the cache folder.
    #     Load the object from the old folder, compute the hash of the object and rename the folder.
    #     """
    #     old_folder = Path(old_folder)

    #     old_folder_name = old_folder.name

    #     params_folder = old_folder / "params.json"
    #     if not params_folder.exists():
    #         raise FileNotFoundError(f"File {params_folder} does not exist")

    #     params = params_from_json(params_folder)

    #     old_hash = old_folder_name.split("_")[-1]
    #     new_hash = cls.hash(params)

    #     if old_hash == new_hash:
    #         print("Hashes are the same, nothing to do")
    #         return

    #     old_components = old_folder_name.split("_")[:-1]
    #     new_folder_name = "_".join(old_components + [new_hash])

    #     shutil.move(old_folder, old_folder.parent / new_folder_name)
    #     print(f"Renamed {old_folder_name} to {new_folder_name}")
