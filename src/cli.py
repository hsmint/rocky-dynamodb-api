from database import Database

def get_item(db : Database, opt : dict) -> None:
    if opt["key"]:
        print(f'Item key {opt["key"]}')
    elif opt["epoch"]:
        print(f'Item epoch {opt["epoch"]}')
    elif opt["block"]:
        print(f'Item block {opt["block"]}')
    elif opt["bitmap"]:
        print(f'Item bitmap {opt["bitmap"]}')

def cli(opt : dict) -> None:
    db = Database(opt["name"])
    data = None
    if opt["list"]:
        print("List")
    elif opt["item"]:
        data = get_item(db, opt)
