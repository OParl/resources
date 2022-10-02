from pathlib import Path
from typing import Any, Dict

from ruamel.yaml import YAML


def sorter(x: Dict[str, Any]):
    if wd := x.get("wd"):
        wd = int(wd[1:])
    else:
        wd = 10**10  # high
    return wd, x["title"]


yaml = YAML()
data = yaml.load(Path("endpoints.yml").read_text())
data.sort(key=sorter)
with Path("endpoints.yml").open("w") as fp:
    yaml.dump(data, fp)
