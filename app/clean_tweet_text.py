import re
import pandas as pd

entities = re.compile("^rt\s|https?://\S+\s?|@\S+\s?|\#\S+\s|[^\w\s]")


def clean_text(text: str, stop_words: list) -> str:
    text = text.lower()
    text = re.sub("\n|\t|\r", " ", text)
    text = re.sub(entities, "", text)
    original, replace = "áéíóúü", "aeiouu"
    translate_table = str.maketrans(original, replace)
    text = text.translate(translate_table)
    text = text.strip()
    text = text.split(" ")
    text = [token for token in text if token not in stop_words]
    text = " ".join(text)
    return text
