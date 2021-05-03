import pathlib

def mkdirs(path):
    return pathlib.Path(path).mkdir(parents=True, exist_ok=True)
