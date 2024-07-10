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
    data = None
    db = Database()
    if opt["list"]:
        print("List")
    elif opt["item"]:
        db.set_name(opt["name"])
        data = get_item(db, opt)
