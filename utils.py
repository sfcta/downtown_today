import altair as alt
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
        (pl.col(col) / pl.col(col).sum().over("year", "geography")).alias(
            f"{col}-industry_share"
        )
    )


def line_plot(df, x, y, color, title="", tooltip=[]):
    if title == "":
        title = y
    if x in {"year", "Year"}:
        df = df.with_columns(date=pl.date(pl.col(x), 1, 1))
        x_axis = alt.X("date", title=x)
    else:
        x_axis = x
    return (
        alt.Chart(df.sort(x))
        .mark_line()
        .encode(x=x_axis, y=alt.Y(y, title=title), color=color, tooltip=tooltip)
        .interactive()
    )


def line_plot_normalized(
    df,
    x,
    y,
    color,
    title=None,
    norm_x_value=2019,
    tooltip=[],
    mark_line_args={},
    chart_args={},
):
    normalized_y_col_name = f"{y} (% of {norm_x_value} value)"
    normalized_df = add_normalization_col(
        df, x, y, normalized_y_col_name, norm_x_value=norm_x_value
    ).sort(x)
    if title is None:
        title = normalized_y_col_name
    else:
        title += f" (% of {norm_x_value} value)"
    if x in {"year", "Year"}:  # make the x axis pretty
        normalized_df = normalized_df.with_columns(date=pl.date(pl.col(x), 1, 1))
        # don't show x axis because graph not starting at 0 on y axis
        x_axis = alt.X("date:T", title=x, axis=alt.Axis(domain=False))
    else:
        # x domain = False: don't show x axis because graph not starting at 0 on y axis
        x_axis = alt.X(x, domain=False, axis=alt.Axis(domain=False))
    chart = (
        alt.Chart(normalized_df)
        .mark_line(**mark_line_args)
        .encode(
            x=x_axis,
            y=alt.Y(normalized_y_col_name, title=title)
            .scale(zero=False)
            .axis(format="%"),
            color=color,
            tooltip=tooltip,
            **chart_args,
        )
        + alt.Chart(pl.DataFrame({x: [norm_x_value]}))
        .mark_rule(color="grey", opacity=0.5, strokeWidth=2)
        .encode(x=x_axis)
        + alt.Chart(pl.DataFrame({normalized_y_col_name: [1]}))
        .mark_rule(color="grey", opacity=0.5, strokeWidth=2)
        .encode(y=normalized_y_col_name)
    ).interactive()
    return normalized_df, chart


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
