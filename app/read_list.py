def get_ls_from_txt(path: str) -> list:
    """Get a list from a txt file.

    Reads a txt file and returns a list of str.

    Parameters
    ----------
        path: str
            absolute path to the text file
    Returns
    -------
        list:
            list of strings
    """
    with open(path, "r") as file:
        tags = file.read().splitlines()
    return tags
