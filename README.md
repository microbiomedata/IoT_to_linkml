# badexperiment
Convert Index of Terms to LinkML YAML. Many moving parts = bad experiment.

## Publishing
Don't forget to update `version` in `[tool.poetry]`

```shell
setopt HIST_IGNORE_SPACE
export pypi_user='mamillerpa'
 export pypi_pw='<SECRET>'
poetry build
poetry publish --username $pypi_user --password $pypi_pw
```

## Installation outside of this repo
Aggressive cleanup?

```shell
# assume we're inside a venv virtual environment
deactivate
rm -rf venv
# purge under what circumstances?
python3.9 -m pip cache purge
python3.9 -m venv venv
source venv/bin/activate
python3.9 -m pip install --upgrade pip
pip install wheel
# check status of package under development
# don't continue with installation when
pip index versions badexperiment
# installation of pandas is slow
#   platform dependent? M1 MBA
pip install badexperiment
```

## Usage outside of this repo

```shell
becli --help
```


