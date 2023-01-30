import time
import pandas as pd
from datetime import date, timedelta
from google.cloud import bigquery
from threading import Thread

import gspread
from google.oauth2.service_account import Credentials
from gspread_dataframe import set_with_dataframe

from constants import SPREADSHEET_LINK, CREDENTIALS_PATH


def save_to_spreadsheet(df, path_to_file, spreadsheet_url, sheet_name):
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    credentials = Credentials.from_service_account_file(path_to_file, scopes=scopes)
    gc = gspread.authorize(credentials)

    # open a google sheet
    gs = gc.open_by_url(spreadsheet_url)

    # select a work sheet from its name
    worksheet = gs.worksheet(sheet_name)

    # write to dataframe
    worksheet.clear()
    set_with_dataframe(worksheet=worksheet, dataframe=df, include_index=True,
                       include_column_header=True, resize=True)


def get_dates_list():
    dates = []
    start_date = date(2016, 8, 1)
    end_date = date(2017, 8, 1)
    delta = end_date - start_date  # returns timedelta

    for i in range(delta.days + 1):
        day = start_date + timedelta(days=i)
        dates.append(str(day).replace("-", ""))
    return dates


def add_new_columns(df):
    # adding columns of device
    df["browser"] = [row["browser"] for row in df["device"]]
    df["operatingSystem"] = [row["operatingSystem"] for row in df["device"]]
    df["isMobile"] = [row["isMobile"] for row in df["device"]]

    # adding columns of geoNetwork
    df["continent"] = [row["continent"] for row in df["geoNetwork"]]
    df["country"] = [row["country"] for row in df["geoNetwork"]]

    # adding columns of total
    df["timeOnSite"] = [row["timeOnSite"] for row in df["totals"]]
    df["hits"] = [row["hits"] for row in df["totals"]]
    return df


def table_aggregation(df):
    # aggregating values for tables
    df_browsers = df[["isMobile", "browser", "continent", "timeOnSite", "hits"]].groupby("browser").agg({
        "isMobile": "mean",
        "continent": pd.Series.mode,
        "timeOnSite": "mean",
        "hits": "mean"})

    df_counties = df[["isMobile", "browser", "country", "timeOnSite", "hits"]].groupby("country").agg({
        "isMobile": "mean",
        "browser": pd.Series.mode,
        "timeOnSite": "mean",
        "hits": "mean"})

    df_channel_grouping = df[["channelGrouping", "isMobile", "browser",
                              "continent", "timeOnSite", "hits"]].groupby("channelGrouping").agg({
        "isMobile": "mean",
        "continent": pd.Series.mode,
        "browser": pd.Series.mode,
        "timeOnSite": "mean",
        "hits": "mean"})

    # getting percent from value
    df_counties["isMobile"] = df_counties["isMobile"] * 100
    df_browsers["isMobile"] = df_browsers["isMobile"] * 100
    df_channel_grouping["isMobile"] = df_channel_grouping["isMobile"] * 100

    # renaming columns
    df_browsers.columns = ["Percent of mobile users",
                           "Most common continent",
                           "Average time on site",
                           "Average amount of hits"]
    df_counties.columns = ["Percent of mobile users",
                           "Most common browser",
                           "Average time on site",
                           "Average amount of hits"]
    df_channel_grouping.columns = ["Percent of mobile users",
                                   "Most common continent",
                                   "Most common browser",
                                   "Average time on site",
                                   "Average amount of hits"]
    # rounding values of the tables
    df_counties = df_counties.round(2)
    df_channel_grouping = df_channel_grouping.round(2)
    df_browsers = df_browsers.round(2)
    return df_counties, df_browsers, df_channel_grouping


def fetch(client, query, df_list):
    df = client.query(query).to_dataframe()
    df_list.append(df)


def batch(iterable, n=1):
    l = len(iterable)
    for ndx in range(0, l, n):
        yield iterable[ndx:min(ndx + n, l)]


def main():
    client = bigquery.Client.from_service_account_json("dulcet-metric-321109-0d45ee97d084.json")
    # query to select table of specific date
    query = """
        SELECT visitNumber, visitId, visitStartTime, date, totals, device, geoNetwork, fullVisitorId,
        channelGrouping
           FROM `bigquery-public-data.google_analytics_sample.ga_sessions_{}` 
    """
    dates = get_dates_list()
    df_list = []
    threads = []
    # getting dates into batches
    for date_batch in batch(dates, 31):
        # creating query to union several tables to make one request
        main_query = []
        for date in date_batch:
            main_query.extend([query.format(date), "\nUNION ALL\n"])
        main_query.pop()
        main_query = "\n".join(main_query)
        # adding thread to list with threads
        threads.append(Thread(target=fetch, args=(client, main_query, df_list)))

    # starting threads and activating them
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    # creating one df
    df = pd.concat(df_list, ignore_index=True)

    df = add_new_columns(df)
    # getting aggregated df
    df_countries, df_browsers, df_channels = table_aggregation(df)

    output_df_list = [(df_browsers, "Browsers"), (df_channels, "Channels"),
                      (df_countries, "Countries")]

    # writing results to spreadsheet
    spreadsheet_threads = [Thread(target=save_to_spreadsheet,
                                  args=(df, CREDENTIALS_PATH, SPREADSHEET_LINK, spreadsheet_name))
                           for df, spreadsheet_name in output_df_list]

    # writing values to spreadsheets with threads
    for s_thread in spreadsheet_threads:
        s_thread.start()

    for s_thread in spreadsheet_threads:
        s_thread.join()


if __name__ == "__main__":
    start_time = time.time()

    main()

    end_time = time.time()
    print(f"Scrapping time {end_time - start_time :0.2f} seconds.")
