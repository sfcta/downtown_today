import sys
from collections import OrderedDict
from functools import reduce
from operator import or_
from pathlib import Path

import altair as alt
import polars as pl

sys.path.append("../")
from altair_utils import color_sfcore_restofsf_restofbayarea, line_strokeDash

county_to_station = {
    "San Francisco": ["EMBR", "MONT", "POWL", "CIVC", "16TH", "24TH", "GLEN", "BALB"],
    "San Mateo": [
        "DALY",
        "COLM",
        "SSAN",
        "SBRN",
        "SFIA",
        "MLBR",
    ],
    "Contra Costa": [
        "RICH",
        "ORIN",
        "LAFY",
        "WCRK",
        "CONC",
        "NCON",
        "PITT",
        "ANTC",
        "DELN",
        "PHIL",
        "PCTR",
        "PLZA",
    ],
    "Alameda": [
        "WOAK",
        "12TH",
        "19TH",
        "MCAR",
        "ASHB",
        "DUBL",
        "WDUB",
        "CAST",
        "WARM",
        "UCTY",
        "SHAY",
        "HAYW",
        "BAYF",
        "SANL",
        "OAKL",
        "COLS",
        "FTVL",
        "LAKE",
        "ROCK",
        "DBRK",
        "NBRK",
        "FRMT",
    ],
    "Santa Clara": [
        "MLPT",
        "BERY",
    ],
}
station_to_county = reduce(  # reduce: union of all the dicts
    or_,  # union (`|`) operator
    (
        {station: county for station in stations}
        for county, stations in county_to_station.items()
    ),
)

# filters
filter_orig_sf = pl.col("origin_county") == "San Francisco"
filter_dest_sf = pl.col("destination_county") == "San Francisco"
filter_od_sf = (pl.col("origin_county") == "San Francisco") | (
    pl.col("destination_county") == "San Francisco"
)
sf_market_st_stations = ["EMBR", "MONT", "POWL", "CIVC"]
filter_orig_sf_market_st = pl.col("origin").is_in(sf_market_st_stations)
filter_dest_sf_market_st = pl.col("destination").is_in(sf_market_st_stations)
filter_od_sf_market_st = (pl.col("origin").is_in(sf_market_st_stations)) | (
    pl.col("destination").is_in(sf_market_st_stations)
)
sf_ex_market_st_stations = ["16TH", "24TH", "GLEN", "BALB"]
filter_orig_sf_ex_market_st = pl.col("origin").is_in(sf_ex_market_st_stations)
filter_dest_sf_ex_market_st = pl.col("destination").is_in(sf_ex_market_st_stations)
filter_od_sf_ex_market_st = (pl.col("origin").is_in(sf_ex_market_st_stations)) | (
    pl.col("destination").is_in(sf_ex_market_st_stations)
)
sf_mission_stations = ["16TH", "24TH"]
filter_od_sf_mission = (pl.col("origin").is_in(sf_mission_stations)) | (
    pl.col("destination").is_in(sf_mission_stations)
)
filter_montofri = pl.col("dow").is_in({1, 2, 3, 4, 5})
filter_satsun = pl.col("dow").is_in({6, 7})
filter_monfri = pl.col("dow").is_in({1, 5})
filter_tuetothu = pl.col("dow").is_in({2, 3, 4})
# filter_fullweek: similar to dow_filter=None in that dow_filtered_hourly_od_df is not
# filtered for dow, but do not include "dow" in temporal_groups
filter_fullweek = True

dow_filters_dict = OrderedDict(
    [
        ("mon", pl.col("dow").is_in({1})),
        ("tue-thu", filter_tuetothu),
        ("fri", pl.col("dow").is_in({5})),
        ("sat/sun", filter_satsun),
    ]
)


geo_orig_filters_for_stacking_dict = {
    "SF (Market St)": filter_orig_sf_market_st,
    "rest of SF": filter_orig_sf & ~filter_orig_sf_market_st,
    "rest of BART system": ~filter_orig_sf,
}


geo_dest_filters_for_stacking_dict = {
    "SF (Market St)": filter_dest_sf_market_st,
    "rest of SF": filter_dest_sf & ~filter_dest_sf_market_st,
    "rest of BART system": ~filter_dest_sf,
}


geo_od_filters_for_stacking_dict = {
    "SF (Market St)": filter_od_sf_market_st,
    "rest of SF": filter_od_sf & ~filter_od_sf_market_st,
    "rest of BART system": ~filter_od_sf,
}


geo_orig_filters_no_stacking_dict = {
    "SF (Market St)": filter_orig_sf_market_st,
    "SF": filter_orig_sf,
    "BART full system": True,
}


geo_dest_filters_no_stacking_dict = {
    "SF (Market St)": filter_dest_sf_market_st,
    "SF": filter_dest_sf,
    "BART full system": True,
}


geo_od_filters_no_stacking_dict = {  # o or d in geographies
    "SF (Market St)": filter_od_sf_market_st,
    "SF": filter_od_sf,
    "BART full system": True,
}


def read_hourly_od_csvs(years):
    """read multiple years of raw BART hourly O-D CSVs"""
    return pl.concat(read_hourly_od_csv(year) for year in years)


def read_hourly_od_csv(year):
    dir = Path("Q:/Data/Observed/Transit/BART/hourly_ridership-od")
    try:
        return _read_hourly_od_csv(dir / f"date-hour-soo-dest-{year}.csv.gz")
    except FileNotFoundError:
        return _read_hourly_od_csv(dir / f"date-hour-soo-dest-{year}.csv")


def _read_hourly_od_csv(filepath):
    return (
        pl.read_csv(
            filepath,
            has_header=False,
            new_columns=["date", "hour", "origin", "destination", "ridership"],
        )
        .with_columns(pl.col("date").str.to_date("%Y-%m-%d"))
        .with_columns(dow=pl.col("date").dt.weekday())  # day of week
        # TODO do we want to calculate is_weekday ?
        # .with_columns(
        #     is_weekday=pl.col("dow")
        #     .replace({i: True for i in range(1, 6)} | {i: False for i in [6, 7]})
        #     .cast(bool)
        # )
        .sort("date")
    )


def group_hourly_od_df(
    hourly_od_df,
    geo_filters_dict,
    time_aggregation="1y",
    dow_filter=None,
    hourly=False,
):
    temporal_groups = _get_temporal_groups(
        time_aggregation=time_aggregation, dow_filter=dow_filter
    )  # ["year", "month" (if time_aggregation == "1mo"), "dow" (if dow_filter is None)]
    return ridership_for_geographies(
        _temporally_group_hourly_od_df(hourly_od_df, dow_filter, temporal_groups),
        geo_filters_dict,
        temporal_groups,
        hourly=hourly,
    )


def temporally_group_hourly_od_df(hourly_od_df, time_aggregation="1y", dow_filter=None):
    """_summary_

    Parameters
    ----------
    hourly_od_df : pl.DataFrame
        output of read_hourly_od_csv() or read_hourly_od_csvs()
    time_aggregation : str, optional
        "1y" or "1mo", by default "1y"
    dow_filter : optional
        day-of-week filter (e.g. filter_satsun for weekend only),
        by default None (keep each day-of-week separate).
        This affects the denominator weights for calculating avg_daily_ridership

    Returns
    -------
    pl.DataFrame
        temporally_grouped_df
    """
    temporal_groups = _get_temporal_groups(
        time_aggregation=time_aggregation, dow_filter=dow_filter
    )  # ["year", "month" (if time_aggregation == "1mo"), "dow" (if dow_filter is None)]
    return _temporally_group_hourly_od_df(hourly_od_df, dow_filter, temporal_groups)


def _temporally_group_hourly_od_df(hourly_od_df, dow_filter, temporal_groups):
    """_summary_

    Parameters
    ----------
    hourly_od_df : pl.DataFrame
        output of read_hourly_od_csv() or read_hourly_od_csvs()
    temporal_groups : {str: pl.Expr}
        output of _get_temporal_groups()

    Returns
    -------
    pl.DataFrame
        temporally_grouped_df
    """
    hourly_od_df = hourly_od_df.with_columns(temporal_groups.values())
    if dow_filter is None:
        dow_filtered_hourly_od_df = hourly_od_df
    else:
        dow_filtered_hourly_od_df = hourly_od_df.filter(dow_filter)

    temporally_grouped_df = dow_filtered_hourly_od_df.group_by(
        "origin", "destination", "hour", *temporal_groups.keys()
    ).agg(pl.sum("ridership"))
    return (
        _join_avg_daily_ridership(
            temporally_grouped_df, dow_filtered_hourly_od_df, temporal_groups
        ).with_columns(
            origin_county=pl.col("origin").replace(station_to_county),
            destination_county=pl.col("destination").replace(station_to_county),
        )
        # .drop("date")
    )


# def group_annual(df):
#     return _group_time_period(df, "1y").with_columns(
#         year=pl.col("date").dt.year(),
#     )


# def group_monthly(df, year):
#     return (
#         _group_time_period(df, "1mo")
#         .with_columns(
#             year=pl.col("date").dt.year(),
#             month=pl.col("date").dt.month(),
#         )
#         .join(
#             # add num_of_dow_in_month column (indexed by [year, month, dow])
#             _year_month_num_of_dow(year),
#             on=["year", "month", "dow"],
#             how="left",
#         )
#     )


# def _group_time_period(df, time_aggregation):
#     _time_aggregation_check(time_aggregation)
#     _date_check(df)
#     return (
#         df.group_by_dynamic(
#             "date",
#             every=time_aggregation,
#             group_by=["origin", "destination", "dow", "hour"],
#         )
#         .agg(
#             # pl.first() as these cols are determined by one of the group_by cols
#             pl.first("origin_county", "destination_county", "is_weekday"),
#             pl.sum("ridership"),
#         )
#         .drop("date")
#     )


# def _date_check(df):
#     # only needed if we do group_by_dynamic("date", ...),
#     # not needed for group_by("year", "month", ...) etc
#     min_date = df.select(pl.min("date")).item()
#     if not ((min_date.month == 1) & (min_date.day == 1)):
#         raise NotImplementedError(
#             'group_by_dynamic("date", ...) later assumes the df starts on Jan 1'
#         )
#     return


def _join_avg_daily_ridership(grouped_df, dow_filtered_hourly_od_df, temporal_groups):
    weights_df = _calculate_avg_daily_inverse_weights(
        dow_filtered_hourly_od_df, temporal_groups
    )
    return (
        grouped_df.join(weights_df, on=temporal_groups.keys(), how="left")
        .with_columns(
            avg_daily_ridership=(pl.col("ridership") / pl.col("num_dow_days"))
        )
        .drop("num_dow_days")
    )


def _time_aggregation_check(time_aggregation):
    if time_aggregation not in {"1y", "1mo"}:
        raise NotImplementedError("time_aggregation must be '1y' or '1mo'.")
    return


def _get_temporal_groups(time_aggregation="1y", dow_filter=None):
    # temporal_groups: temporal indices for joining weights_df to grouped_df
    _time_aggregation_check(time_aggregation)
    temporal_groups = {"year": pl.col("date").dt.year().alias("year")}
    if time_aggregation == "1mo":
        temporal_groups |= {"month": pl.col("date").dt.month().alias("month")}
    if dow_filter is None:
        temporal_groups |= {"dow": "dow"}
    return temporal_groups


def _calculate_avg_daily_inverse_weights(
    dow_filtered_hourly_od_df, temporal_categories
):
    """caltulate the weights to divide ridership by to get avg_daily_ridership

    Parameters
    ----------
    dow_filtered_hourly_od_df : pl.DataFrame
        output of read_hourly_od_csv(),
        filtered by dow_filter (if dow_filter is not None)
    temporal_categories : [str]
        [
            "year",
            "month",  # if time_aggregation == "1mo"
            "dow"  # if dow_filter is None, i.e. keep each day-of-week separate
        ]

    Returns
    -------
    weights_df
        df of denominator weights (in column `num_dow_days`),
        indexed by temporal_categories, i.e.
        - year
        - month (if time_aggregation == "1mo")
        - dow (if dow_filter is None, i.e. keep each day-of-week separate)
    """
    # count unique days in raw dataset (or the raw dataset filtered by dow_filter),
    # which can capture days that BART is closed
    # (assumes there's always ridership somewhere on each day that BART is running)
    return dow_filtered_hourly_od_df.group_by(temporal_categories).agg(
        num_dow_days=pl.col("date").unique().len()
    )


# def _num_of_dow_in_month(year, month):
#     """[no. of Mondays, no. of Tuesdays, ..., no. of Sundays] in that year-month}"""
#     return np.array(calendar.monthcalendar(year, month)).astype(bool).sum(axis=0)


# def _year_month_num_of_dow(year):
#     months = range(1, 13)
#     return pl.DataFrame(
#         {
#             "year": year,
#             "month": chain(*zip(*repeat(months, 7))),  # [1]*7 + [2]*7 + ... + [12]*7
#             "dow": list(range(1, 8)) * 12,  # 1-7 repeated 12 times
#             "num_of_dow_in_month": np.concatenate(
#                 [_num_of_dow_in_month(year, month) for month in months]
#             ),
#         },
#         schema={  # to match dtypes of .dt.year/month/dow() for joining later
#             "year": pl.Int32,
#             "month": pl.Int8,
#             "dow": pl.Int8,
#             "num_of_dow_in_month": pl.Int16,
#         },
#     )


def ridership_for_geographies(
    temporally_grouped_df, geo_filters_dict, temporal_groups, hourly=False
):
    """Calculate results for different geographies from temporally_grouped_df

    Parameters
    ----------
    temporally_grouped_df : pl.DataFrame
        output of temporally_group_hourly_od_df()
    geo_filters_dict : {str, pl.Expr}
        dict of geographical filters, e.g. {"full_system": True, "sf", filter_orig_sf}
        (use True to _not_ filter the df)
    temporal_groups : {str: pl.Expr}
        output of _get_temporal_groups()

    Returns
    -------
    pl.DataFrame
        temporally grouped df with the results for each geographyt vertically stacked
    """
    temporal_groups = temporal_groups.keys()
    if hourly:
        temporal_groups = list(temporal_groups) + ["hour"]
    return pl.concat(
        (
            temporally_grouped_df.filter(geo_filter)
            .group_by(temporal_groups)
            .agg(pl.sum("ridership", "avg_daily_ridership"))
            .with_columns(geography=pl.lit(geog))
            for geog, geo_filter in (geo_filters_dict).items()
        )
    )


def group_hourly_od_dow_filtered_dfs(
    hourly_od_df,
    geo_filters_dict,
    time_aggregation="1y",
    dow_filters_dict=dow_filters_dict,
    hourly=False,
    shares=False,
):
    df_dict = OrderedDict()
    for dow_filter_name, dow_filter in dow_filters_dict.items():
        df = group_hourly_od_df(
            hourly_od_df,
            geo_filters_dict,
            time_aggregation=time_aggregation,
            dow_filter=dow_filter,
            hourly=hourly,
        )
        if shares:
            df = df.with_columns(
                full_sys_avg_daily_ridership=pl.sum("avg_daily_ridership").over(
                    "year",
                    # "dow",  # no need dow since already filtered out
                    "hour",
                )  # sum up the avg ridership of the 3 geographies
            ).select(
                "year",
                # "dow",
                "hour",
                "avg_daily_ridership",
                "geography",
                avg_daily_ridership_geog_share=(
                    pl.col("avg_daily_ridership")
                    / pl.col("full_sys_avg_daily_ridership")
                ),
            )
        df_dict[dow_filter_name] = df
    return df_dict


def plot_annual_ridership_over_day_by_geog(annual_dow_filtered_df, year, title):
    return (
        annual_dow_filtered_df.filter(pl.col("year") == year)
        .sort("hour")
        .plot.line(x="hour", y="avg_daily_ridership", color="geography")
        .properties(title=title)
    )


def plot_annual_ridership_over_day_by_year(annual_dow_filtered_df, title):
    return (
        alt.Chart(annual_dow_filtered_df)
        .mark_line()
        .encode(
            x=alt.X("hour", scale=alt.Scale(domain=[0, 23])),
            y="avg_daily_ridership",
            color="year:N",
            strokeDash="geography",
        )
        .properties(title=title)
    )


def plot_annual_ridership_over_day_grid(
    hourly_od_df, years, geo_filters_no_stacking_dict, dow_filters_dict=dow_filters_dict
):
    annual_dow_filtered_dfs = group_hourly_od_dow_filtered_dfs(
        hourly_od_df,
        geo_filters_no_stacking_dict,
        time_aggregation="1y",
        dow_filters_dict=dow_filters_dict,
        hourly=True,
    )
    return alt.hconcat(
        *(
            alt.layer(
                *(
                    plot_annual_ridership_over_day_by_year(
                        annual_dow_filtered_df,
                        f"BART {y} avg daily ridership ({dow_filter_name})",
                    )
                    for y in years
                )
            )
            for (
                dow_filter_name,
                annual_dow_filtered_df,
            ) in annual_dow_filtered_dfs.items()
        )
    )
    # code for hvplot, which we're no longer using:
    # return (
    #     hv.Layout(
    #         list(
    #             chain.from_iterable(
    #                 [
    #                     plot_annual_ridership_over_day(
    #                         annual_dow_filtered_df,
    #                         y,
    #                         f"BART {y} avg daily ridership ({dow_filter_name})",
    #                     )
    #                     for (
    #                         dow_filter_name,
    #                         annual_dow_filtered_df,
    #                     ) in annual_dow_filtered_dfs.items()
    #                 ]
    #                 for y in years
    #             )
    #         )
    #     )
    #     .cols(len(dow_filters_dict))
    #     .opts(shared_axes=False)  # not sure how to only share x but not y-axis)
    # )


def plot_annual_ridership_shares_over_day_grid(
    hourly_od_df,
    geo_filters_for_stacking_dict,
    # dow_filters_dict=dow_filters_dict,  # hardcode
    weekday_only=False,
    pre_plot_filter=True,
    title_note="",
):
    dow_filters_weekday_sat_sun = OrderedDict(
        [
            ("Weekday", filter_montofri),
            ("Sat", pl.col("dow").is_in({6})),
            ("Sun", pl.col("dow").is_in({7})),
        ]
    )
    # BART service hours: 5/6/8am-Midnight on weekdays/Sat/Sun
    # the SF share spikes in the first hour probably due to
    # the SF stations starting service before the suburban stations
    x_domains = [[6, 23], [7, 23], [9, 23]]
    if weekday_only:
        dow_filters_weekday_sat_sun = {"Weekday": filter_montofri}
        x_domains = [[6, 23]]
    annual_dow_filtered_dfs = group_hourly_od_dow_filtered_dfs(
        hourly_od_df,
        geo_filters_for_stacking_dict,
        time_aggregation="1y",
        dow_filters_dict=dow_filters_weekday_sat_sun,
        hourly=True,
        shares=True,
    )
    return alt.hconcat(
        *(
            alt.Chart(annual_dow_filtered_df.filter(pre_plot_filter))
            .mark_line(clip=True)
            .encode(
                x=alt.X("hour", scale=alt.Scale(domain=x_domain)),
                y="avg_daily_ridership_geog_share",
                color=alt.Color("geography").scale(
                    domain=["SF (Market St)", "rest of SF", "rest of BART system"],
                    range=color_sfcore_restofsf_restofbayarea,
                ),
                # strokeDash sort: the more solid lines should be for more recent years
                strokeDash=alt.StrokeDash("year", sort="descending").scale(
                    range=line_strokeDash
                ),
            )
            .properties(
                title={
                    "text": "BART avg daily ridership shares",
                    "subtitle": f"({dow_filter_name}, {title_note})",
                }
            )
            for ((dow_filter_name, annual_dow_filtered_df), x_domain) in zip(
                annual_dow_filtered_dfs.items(), x_domains
            )
        )
    ).resolve_scale(y="shared")


# def plot_annual_time_period_ridership_by_geo(
#     grouped_for_stacking_annual_dow_df, year, title, shares=False
# ):
#     if "hour" not in grouped_for_stacking_annual_dow_df:
#         raise RuntimeError("Run group_hourly_od_df() with hourly=True.")
#     index_cols = ["geography", "time_period"]
#     df = (
#         grouped_for_stacking_annual_dow_df.filter(pl.col("year") == year)
#         .with_columns(
#             # time periods selected based on 2023 Mon-Fri ridership data
#             time_period=pl.when((6 <= pl.col("hour")) & (pl.col("hour") <= 10))
#             .then(pl.lit("06-10"))
#             .when((11 <= pl.col("hour")) & (pl.col("hour") <= 14))
#             .then(pl.lit("11-14"))
#             .when((15 <= pl.col("hour")) & (pl.col("hour") <= 19))
#             .then(pl.lit("15-19"))
#             .otherwise(pl.lit("20-05"))
#         )
#         .group_by("year", *index_cols)
#         .agg(pl.sum("avg_daily_ridership"))
#     )
#     if shares:
#         df = df.with_columns(
#             avg_daily_ridership_in_geog=pl.sum("avg_daily_ridership").over("geography")
#         ).select(
#             index_cols,
#             avg_daily_ridership_share=(
#                 pl.col("avg_daily_ridership") / pl.col("avg_daily_ridership_in_geog")
#             ),
#         )
#     else:
#         df = df.select(index_cols + ["avg_daily_ridership"])
#     return (
#         df.to_pandas()
#         .set_index(index_cols)
#         .sort_index()
#         .hvplot.bar(stacked=True, title=title)
#     )


# def plot_annual_time_period_ridership_by_geo_grid(
#     hourly_od_df, years, geo_filters_no_stacking_dict, dow_filters_dict=dow_filters_dict
# ):
#     annual_dow_filtered_dfs = group_hourly_od_dow_filtered_dfs(
#         hourly_od_df,
#         geo_filters_no_stacking_dict,
#         time_aggregation="1y",
#         dow_filters_dict=dow_filters_dict,
#         hourly=True,
#     )
#     return (
#         hv.Layout(
#             list(
#                 chain.from_iterable(
#                     [
#                         plot_annual_time_period_ridership_by_geo(
#                             annual_dow_filtered_df,
#                             y,
#                             f"BART {y} avg daily ridership shares in each time period ({dow_filter_name})",
#                             shares=True,
#                         )
#                         for (
#                             dow_filter_name,
#                             annual_dow_filtered_df,
#                         ) in annual_dow_filtered_dfs.items()
#                     ]
#                     for y in years
#                 )
#             )
#         )
#         .cols(len(dow_filters_dict))
#         .opts(shared_axes=False)  # not sure how to only share x but not y-axis)
#     )


# def plot_monthly_ridership_over_week(df, title):
#     return (
#         df.group_by(["year", "month", "dow"])
#         .agg(pl.sum("avg_daily_ridership"))
#         .sort("dow")
#         .hvplot.line(
#             x="dow",
#             y="avg_daily_ridership",
#             by="month",
#             alpha=0.7,
#             title=title,
#         )
#     )


# def plot_monthly_ridership_over_day(df, title):
#     return (
#         df.group_by(["year", "month", "hour"])
#         .agg(pl.sum("avg_daily_ridership"))
#         .sort("month", "hour")
#         .hvplot.line(
#             x="hour",
#             y="avg_daily_ridership",
#             by="month",
#             alpha=0.7,
#             title=title,
#         )
#     )
