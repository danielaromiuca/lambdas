import re

arg_locations = re.compile("bs.*as|buenos aires|caba|arg\.|argentina")


def check_location_arg(text: str) -> bool:
    x = re.search(arg_locations, text.lower())
    if x:
        return True
    return False
