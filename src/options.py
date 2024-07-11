from typing import Tuple

import argparse
import sys

def get_argument() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="CLI for dynamodb")
    parser.add_argument("-l", "--list", help="List tables in dynamodb", action="store_true")
    parser.add_argument("-i", "--item", help="List items in dynamodb", action="store_true")

    parser.add_argument("-n", "--name", help="Dynamodb name", default=None, type=str)

    parser.add_argument("--key", help="Get item table by key", default=None, type=str)
    parser.add_argument("--epoch", help="Get item table by epoch", default=None, type=int)
    parser.add_argument("--block", help="Get item table by block", default=None, type=int)
    parser.add_argument("--bitmap", help="Get item by bitmap need epoch", default=None, type=int)

    parser.add_argument("-o", "--output", help="Output file", default=None, type=str)
    return parser.parse_args()

def check_option(args : argparse.Namespace) -> Tuple[dict, bool]:
    opt = { "list": False, "item": False }
    if args.list:
        opt["list"] = True
    if args.item:
        opt["item"] = True
    return opt, opt["list"] or opt["item"]

def item_option(args : argparse.Namespace, opt : dict) -> dict:
    if args.name == None:
        sys.exit("error: dynamodb name is required")
    opt["name"] = args.name
    if args.key != None:
        opt["key"] = { "key" : args.key }
    elif args.epoch != None:
        opt["epoch"] = args.epoch
    elif args.block != None:
        opt["block"] = args.block
    elif args.bitmap != None:
        opt["bitmap"] = args.bitmap
    return opt

def get_options() -> dict:
    args = get_argument()
    opt, has_option = check_option(args)
    opt["output"] = args.output
    if not has_option or (opt["list"] and not opt["item"]):
        return opt
    if opt["list"] and opt["item"]:
        sys.exit("error: cannot use both list and item options")
    opt = item_option(args, opt)
    return opt
