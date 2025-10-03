from dataclasses import dataclass
from pathlib import Path

from dataclass_wizard import JSONWizard


@dataclass
class AbstractParams(JSONWizard):
    pass


def deepcopy(params):
    json = params.to_json()
    copy = AbstractParams.from_json(json)
    return copy


def params_to_json(params, path: Path):
    json = params.to_json(indent=2)
    with open(path, "w") as f:
        f.write(json)


def params_from_json(path: Path):
    with open(path) as f:
        json = f.read()
    params = AbstractParams.from_json(json)
    return params
