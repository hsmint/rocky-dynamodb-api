from database import Database
from typing import Dict

import sys

LIST = 1
ITEM = 2

def get_item(db : Database, opt : Dict) -> None:
    if opt["key"]:
        print(f'Item key {opt["key"]}')
    elif opt["epoch"]:
        print(f'Item epoch {opt["epoch"]}')
    elif opt["block"]:
        print(f'Item block {opt["block"]}')
    elif opt["bitmap"]:
        print(f'Item bitmap {opt["bitmap"]}')

def print_list_tables(data : Dict) -> None:
    for value in data.items():
        print(value)

def print_item(data : Dict) -> None:
    pass # TODO: Implement is needed

def print_to_stdout(data : Dict, type : int) -> None:
    if type == LIST:
        print_list_tables(data)
    elif type == ITEM:
        print_item(data)

def print_to_file(data : Dict, type : int, output : str) -> None:
    with open(output, "w") as sys.stdout:
        print_to_stdout(data, type)


def cli(opt : Dict) -> None:
    data = dict()
    db = Database()
    if opt["list"]:
        data = db.list_tables()
    elif opt["item"]:
        db.set_name(opt["name"])

    type = LIST if opt["list"] else ITEM
    if opt["output"] != None:
        return print_to_file(data, type, opt["output"])
    print_to_stdout(data, type)
