import polars as pl


def add_normalization_col(df, x, y, normalized_y_col_name, norm_x_value=2019):
    norm_col = f"{y}-at_{norm_x_value}"
    join_on = df.select(pl.col("*").exclude(x, y, norm_col)).columns
    return (
        df.join(
            df.filter(pl.col(x) == norm_x_value)
            .rename({y: norm_col})
            .select(join_on + [norm_col]),
            on=join_on,
            how="left",
            coalesce=True,
        )
        .with_columns((pl.col(y) / pl.col(norm_col)).alias(normalized_y_col_name))
        .drop(norm_col)
    )


def add_industry_share_col(df, col):
    """for CES and QCEW"""
    return df.with_columns(
        (pl.col(col) / pl.col(col).sum().over("Year", "geography")).alias(
            f"{col}-industry_share"
        )
    )


def line_plot(df, x, y, by, title, frame_width=None, legend=True):
    return df.sort(x).plot.line(
        x=x, y=y, by=by, title=title, frame_width=frame_width, legend=legend
    )


def line_plot_normalized(
    df, x, y, by, title, norm_x_value=2019, frame_width=None, legend=True
):
    normalized_y_col_name = f"{y}-normalized_to_{norm_x_value}"
    return line_plot(
        add_normalization_col(
            df, x, y, normalized_y_col_name, norm_x_value=norm_x_value
        ),
        x,
        normalized_y_col_name,
        by,
        title=title,
        frame_width=frame_width,
        legend=legend,
    )


def stacked_bar_plot(df, y, geography, by, title, years, frame_width=None, legend=True):
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
            legend=legend,
        )
    )


def paired_line_plots(df, x, y, by, ymin=None, frame_width=None):
    sf_plot = df.filter(pl.col("geography") == "San Francisco").plot.line(
        x=x,
        y=y,
        by=by,
        # stacked=True,
        title="San Francisco",
        ylim=(ymin, None),
        frame_width=frame_width,
        # frame_height=200,
        xticks=df.select(x).unique(),
        legend=False,
    )
    bayarea_plot = df.filter(pl.col("geography") == "Bay Area").plot.line(
        x=x,
        y=y,
        by=by,
        # stacked=True,
        title="Bay Area",
        ylim=(ymin, None),
        frame_width=frame_width,
        # frame_height=200,
        xticks=df.select(x).unique(),
        legend=True,
    )
    return sf_plot + bayarea_plot
