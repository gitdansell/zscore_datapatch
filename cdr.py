# A script to update the CDR (Canadian Depository Receipts) information in Z database from online sources.
import os
import pandas as pd
from dbstuff import get_sqlalchemy_pg_engine


# Both source webpages use Javascript, so there's no direct link to the data. Excel and csv files must be downloaded manually.
# https://www.cboe.com/ca/equities/market-activity/listing-directory/?security-type=deprcpt
# https://cdr.cibc.com/#/cdrDirectory
# TODO: Automate the downloads with Playwright


cibc = os.path.expandvars(r"%USERPROFILE%\Downloads\CIBC_CDR_EN.xlsx" )
cboe = os.path.expandvars(r"%USERPROFILE%\Downloads\cboe listing_directory_data.csv")

df_cboe = pd.read_csv(cboe)
df_cboe = df_cboe[["Symbol", "Name"]]
df_cboe = df_cboe.rename(columns={
    "Symbol": "ticker",
    "Name": "name"
})
df_cboe['name'] = df_cboe['name'].str.replace(' CDR (CAD HEDGED)', '', regex=False)


df_cibc = pd.read_excel(cibc, engine='openpyxl')
df_cibc = df_cibc[["CDR Name", "CDR Ticker", "CDR Ratio", "Country", "Sector"]]
df_cibc = df_cibc.rename(columns={
    "CDR Name": "name_cibc",
    "CDR Ticker": "ticker",
    "CDR Ratio": "cdr_ratio",
    "Country": "country",
    "Sector": "sector"
})
df_cibc["name_cibc"] = df_cibc["name_cibc"].str.replace(" CDR (CAD Hedged)", "", regex=False)

# Find stock symbols unique to CBOE (not in CIBC)
cboe_rightjoin = df_cibc.merge(df_cboe, on="ticker", how="right", indicator=True)
cboe_unique = cboe_rightjoin[cboe_rightjoin['_merge'] == 'right_only']
cboe_unique = cboe_unique[['ticker', 'name']]

# print(cboe_unique.head)

# # inner = df_cibc.merge(df_cboe, on="Symbol", how="inner")
# # print(inner.head)

# # Find stock symbols unique to CIBC (not in CBOE)
# cibc_leftjoin = df_cibc.merge(df_cboe, on="ticker", how="left", indicator=True) 
# cibc_unique = cibc_leftjoin[cibc_leftjoin['_merge'] == 'left_only']

# # Grab only two columns from each dataframe, rename CIBC Name column to match CBOE
# cboe_slim = df_cboe[['Symbol', 'Name']]
# cibc_slim = cibc_unique[['Symbol', 'Name_cibc']]
# cibc_slim = cibc_slim.rename(columns={
#     "Name_cibc": "Name"    
# })

df_cibc = df_cibc.rename(columns={
    "name_cibc": "name"
})

# # Combine both dataframes, ignore original indexes, sort by Symbol
all = pd.concat([df_cibc, cboe_unique], ignore_index=True)

# pd.set_option('display.max_rows', None)  # show all rows
# print(all)
# pd.reset_option

engine = get_sqlalchemy_pg_engine()
all.to_sql('cdr', engine, if_exists='replace', index=False)

