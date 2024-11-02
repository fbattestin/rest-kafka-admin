def parse_nullable_string(s: str) -> str | None:
    """
    Parses a string that may represent a nullable value.

    Args:
        s (str): The string to parse.

    Returns:
        str or None: Returns None if the input string is "None", otherwise returns the input string.
    """
    if s == "None":
        return None
    else:
        return s