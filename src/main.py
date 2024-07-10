from options import get_options
from cli import cli


if __name__ == "__main__":
    opt = get_options()
    if opt["list"] or opt["item"]:
        cli(opt)
    # TODO : Implement needed for future work
