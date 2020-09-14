import os
import json

__all__ = ['config']

pintport_config = os.path.expanduser('~/.pintport/config.json')

try:
    with open(pintport_config) as fobj:
        config = fobj.read()
except FileNotFoundError:
    config = {}
else:
    config = json.loads(config)

