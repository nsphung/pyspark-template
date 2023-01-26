import sys
from typing import List


def main(args: List[str]) -> None:
    """The main routine."""
    if args is None:
        args = sys.argv[1:]

    print("Unuse DE Main Routine.")


if __name__ == "__main__":
    main([])
