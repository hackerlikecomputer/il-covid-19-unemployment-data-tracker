import os
from bs4 import BeautifulSoup
import pandas as pd
from common import get_with_retry

url = "https://www2.illinois.gov/ides/lmi/Pages/Weekly_Claims.aspx"
headers = {
    "Connection": "keep-alive",
    "Cache-Control": "max-age=0",
    "sec-ch-ua": "^\\^Google",
    "sec-ch-ua-mobile": "?0",
    "Upgrade-Insecure-Requests": "1",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    " (KHTML, like Gecko) Chrome/89.0.4389.90 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif"
    ",image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
    "Sec-Fetch-Site": "cross-site",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-User": "?1",
    "Sec-Fetch-Dest": "document",
    "Accept-Language": "en-US,en;q=0.9",
    "If-Modified-Since": "Wed, 17 Mar 2021 21:01:19 GMT",
}
resp = get_with_retry(url, headers=headers)
soup = BeautifulSoup(resp.text, features="lxml")
table = soup.find(
    "table", {"summary": "Weekly Claims: Unemployment Insurance Data for Regular State"}
)
format_int = lambda v: int(v.replace(",", ""))
format_pct = lambda v: int(v.replace("%", ""))
components = [
    {
        "col": "date",
        "selector": "#ctl00_PlaceHolderMain_ctl01__ControlWrapper_RichHtmlField > "
        "table > tbody > tr:nth-child(1) > td:nth-child(2)",
        # change from "week ending" to "week beginning" starting on monday
        # i've been told this is easier to understand
        "func": lambda v: pd.to_datetime(v) - pd.Timedelta(days=5),
    },
    {
        "col": "weekly_claims",
        "selector": "#ctl00_PlaceHolderMain_ctl01__ControlWrapper_RichHtmlField > "
        "table > tbody > tr:nth-child(2) > td:nth-child(2)",
        "func": format_int,
    },
    {
        "col": "weekly_claims_prev_wk_n",
        "selector": "#ctl00_PlaceHolderMain_ctl01__ControlWrapper_RichHtmlField > "
        "table > tbody > tr:nth-child(2) > td:nth-child(4)",
        "func": format_int,
    },
    {
        "col": "weekly_claims_prev_wk_pct",
        "selector": "#ctl00_PlaceHolderMain_ctl01__ControlWrapper_RichHtmlField > "
        "table > tbody > tr:nth-child(2) > td:nth-child(5)",
        "func": format_pct,
    },
    {
        "col": "weekly_claims_prev_yr",
        "selector": "#ctl00_PlaceHolderMain_ctl01__ControlWrapper_RichHtmlField > "
        "table > tbody > tr:nth-child(2) > td:nth-child(6)",
        "func": format_int,
    },
    {
        "col": "weekly_claims_prev_yr_n",
        "selector": "#ctl00_PlaceHolderMain_ctl01__ControlWrapper_RichHtmlField > "
        "table > tbody > tr:nth-child(2) > td:nth-child(7)",
        "func": format_int,
    },
    {
        "col": "weekly_claims_prev_yr_pct",
        "selector": "#ctl00_PlaceHolderMain_ctl01__ControlWrapper_RichHtmlField > "
        "table > tbody > tr:nth-child(2) > td:nth-child(8)",
        "func": format_pct,
    },
]
record = {}
for c in components:
    col_name = c["col"]
    record[col_name] = table.select_one(c["selector"]).text.strip()
    if "func" in c:
        record[col_name] = c["func"](record[col_name])
output_file = "output/ides_weekly_unemployment_claims.csv"
if os.path.exists(output_file):
    archive = pd.read_csv(
        output_file,
        index_col="date",
        converters={
            "date": pd.to_datetime,
            "weekly_claims": lambda v: format_int(v) if v != "" else v,
            "weekly_claims_prev_wk_pct": lambda v: format_pct(v) if v != "" else v,
            "weekly_claims_prev_yr": lambda v: format_int(v) if v != "" else v,
            "weekly_claims_prev_yr_pct": lambda v: format_pct(v) if v != "" else v,
        },
    )
    max_date = archive.index.max()
else:
    archive = pd.DataFrame()
    max_date = pd.Timestamp(1900, 1, 1)
if record["date"] > max_date:
    out = pd.DataFrame(record, index=[0]).set_index("date").append(archive)
else:
    out = archive
out.sort_values("date", ascending=False).to_csv(output_file)