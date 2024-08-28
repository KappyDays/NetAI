import json

def json_read(json_file):
    with open(json_file, 'r') as f:
        return json.load(f)