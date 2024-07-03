import polars as pl


def add_industry_share_col(df, col):
    """for CES and QCEW"""
    return df.with_columns(
        (pl.col(col) / pl.col(col).sum().over("Year", "geography")).alias(
            f"{col}-industry_share"
        )
    )


def line_plot(df, x, y, by, title, frame_width=None):
    return df.sort(x).plot.line(x=x, y=y, by=by, title=title, frame_width=frame_width)


def line_plot_normalized(df, x, y, by, title, norm_x_value=2019, frame_width=None):
    norm_col = f"{x}-{norm_x_value}"
    normalized_y_col = f"{y} (normalized to {norm_x_value})"
    df = df.join(
        df.filter(pl.col(x) == norm_x_value).rename({y: norm_col}),
        on=by,
        how="left",
        coalesce=True,
    ).with_columns((pl.col(y) / pl.col(norm_col)).alias(normalized_y_col))
    return line_plot(df, x, normalized_y_col, by, title=title, frame_width=frame_width)


def stacked_bar_plot(df, y, geography, by, title, years, frame_width=None):
    return (
        df.filter(pl.col("geography") == geography, pl.col("Year").is_in(years))
        .sort("Year", by)
        .plot.bar(
            x="Year",
            y=y,
            by=by,
            stacked=True,
            title=title,
            frame_width=frame_width,
        )
    )
