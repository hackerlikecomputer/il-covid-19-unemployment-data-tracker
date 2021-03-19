import pandas as pd

url = (
    "https://www2.illinois.gov/ides/lmi/Mass%20Layoff%20Statistics%20MLS/"
    "alllayoffs-industry-NAICS.xls"
)
df = pd.read_excel(url, skiprows=8, skipfooter=6, sheet_name=1, na_values=["*"])
df = df.dropna()
df.columns = ["industry", "date", "layoff_events", "initial_claims"]
df = df[~(df.industry.str.contains("Total", na=False))]

df.to_csv("output/ides_monthly_layoff_events_by_industry.csv", index=False)

all_industries = df.groupby("date")[["layoff_events", "initial_claims"]].sum()
all_industries.to_csv("output/ides_monthly_layoff_events_all_industries.csv")

df["date"] = pd.to_datetime(df.date)
pandemic_industry_totals = (
    df[df.date >= "2020-03-01"]
    .groupby("industry")[["layoff_events", "initial_claims"]]
    .sum()
)
pandemic_industry_totals = pandemic_industry_totals[
    pandemic_industry_totals.layoff_events > 0
]
pandemic_industry_totals.to_csv(
    "output/ides_total_layoff_events_by_industry_from_mar20.csv"
)
