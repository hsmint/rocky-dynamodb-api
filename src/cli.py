from database import Database
from typing import List, Dict

import sys

LIST = 1
ITEM = 2

def get_item(db : Database, opt : Dict) -> List[Dict]:
    data = list()
    if "key" in opt:
        data = db.item_by_key(opt["key"])
    elif "epoch" in opt:
        data = db.item_by_epoch(opt["epoch"])
    elif "block" in opt:
        data = db.item_by_block(opt["block"])
    elif "bitmap" in opt:
        data = db.item_by_bitmap_epoch(opt["bitmap"])
    else:
        data = db.item()
    return data

def print_list_tables(data : List[Dict]) -> None:
    for _, value in data[0].items():
        print(value)

def print_item(data : List[Dict]) -> None:
    print("key,value")
    for item in data:
        print(f'{item["key"]},{item["value"]}')

def print_to_stdout(data : List[Dict], type : int) -> None:
    if type == LIST:
        print_list_tables(data)
    elif type == ITEM:
        print_item(data)

def print_to_file(data : List[Dict], type : int, output : str) -> None:
    with open(output, "w") as sys.stdout:
        print_to_stdout(data, type)


def cli(opt : Dict) -> None:
    data = list()
    db = Database()
    if opt["list"]:
        data = db.list_tables()
    elif opt["item"]:
        db.set_name(opt["name"])
        data = get_item(db, opt)
    type = LIST if opt["list"] else ITEM
    if opt["output"] != None:
        return print_to_file(data, type, opt["output"])
    print_to_stdout(data, type)
