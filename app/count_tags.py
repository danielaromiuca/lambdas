import pandas as pd


def tags_count(text: str, economic_tags: list, uncertainty_tags: list) -> list:

    economic_tokens_count = 0
    uncertainty_tokens_count = 0

    tokens_ls = text.split(" ")

    tokens_count = len(tokens_ls)

    for token in tokens_ls:

        if token in economic_tags:
            economic_tokens_count += 1
        if token in uncertainty_tags:
            uncertainty_tokens_count += 1
    return tokens_count, economic_tokens_count, uncertainty_tokens_count


def tags_count_df(
    series: pd.Series, economic_tags: list, uncertainty_tags: list
) -> list:
    token_count_df = series.apply(
        lambda x: tags_count(x, economic_tags, uncertainty_tags)
    )

    return [list(x) for x in zip(*token_count_df)]
