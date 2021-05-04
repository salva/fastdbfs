import pathlib
import subprocess
import json
import logging

def mkdirs(path):
    return pathlib.Path(path).mkdir(parents=True, exist_ok=True)

def call_external_processor_json(cmd, data):
    cp = subprocess.run(cmd,
                        shell=True,
                        input = (json.dumps(data)+"\n").encode("utf-8") ,
                        capture_output=True)
    logging.debug(f"external processor output: {cp.stdout}")
    return json.loads(cp.stdout.decode("utf-8"))
