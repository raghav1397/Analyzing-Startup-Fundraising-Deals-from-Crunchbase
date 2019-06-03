import pandas as pd
pd.options.display.max_columns = 99
crunchbase = pd.read_csv('crunchbase-investments.csv', chunksize=5000, encoding='ISO-8859-1')

mv_list = []
for chunk in crunchbase:
    mv_list.append(chunk.isnull().sum())
    
combined_mv_vc = pd.concat(mv_list)
unique_combined_mv_vc = combined_mv_vc.groupby(combined_mv_vc.index).sum()
unique_combined_mv_vc.sort_values()

crunchbase = pd.read_csv('crunchbase-investments.csv', chunksize=5000, encoding='ISO-8859-1')
counter = 0
series_memory_fp = pd.Series()
for chunk in crunchbase:
    if counter == 0:
        series_memory_fp = chunk.memory_usage(deep=True)
    else:
        series_memory_fp += chunk.memory_usage(deep=True)
    counter += 1

# Drop memory footprint calculation for the index.
series_memory_fp = series_memory_fp.drop('Index')
series_memory_fp

series_memory_fp.sum() / (1024 * 1024)

unique_combined_mv_vc.sort_values()

drop_cols = ['investor_permalink', 'company_permalink', 'investor_category_code']
keep_cols = chunk.columns.drop(drop_cols)

keep_cols.tolist

col_types = {}
crunchbase = pd.read_csv('crunchbase-investments.csv', chunksize=5000, encoding='ISO-8859-1', usecols=keep_cols)

for chunk in crunchbase:
    for col in chunk.columns:
        if col not in col_types:
            col_types[col] = [str(chunk.dtypes[col])]
        else:
            col_types[col].append(str(chunk.dtypes[col]))

uniq_col_types = {}
for k,v in col_types.items():
    uniq_col_types[k] = set(col_types[k])
uniq_col_types

chunk.head()

import sqlite3
conn = sqlite3.connect('crunchbase.db')
crunchbase = pd.read_csv('crunchbase-investments.csv', chunksize=5000, encoding='ISO-8859-1')

for chunk in crunchbase:
    chunk.to_sql("investments", conn, if_exists='append', index=False)

print("Total MB for Crunchbase.db:", 10768384 / 1024**2)

crunch_df = pd.read_sql('select company_category_code, investor_name, raised_amount_usd, funding_round_type from investments;', conn)
print(crunch_df.head())

cmp_type_df = crunch_df.groupby(crunch_df.company_category_code).sum()
cmp_type_df.sort_values('raised_amount_usd', ascending=False, inplace=True) 

print(cmp_type_df.head())

investor_name_df = crunch_df.groupby(crunch_df.investor_name).sum()
investor_name_df.sort_values('raised_amount_usd', ascending=False, inplace=True) 

print(investor_name_df.head())

investor_name_counts = crunch_df.investor_name.value_counts()

investor_name_df["count"] = investor_name_counts
investor_name_df["avg investment"] = investor_name_df['raised_amount_usd'] / investor_name_df["count"] 
investor_name_df.sort_values('raised_amount_usd', ascending=False, inplace=True) 
print(investor_name_df.head())

funding_round_type_counts = crunch_df.funding_round_type.value_counts()
funding_round_type_df = pd.DataFrame()
funding_round_type_df['count'] = funding_round_type_counts
funding_round_type_df.sort_values('count', ascending=False, inplace=True) 

print(funding_round_type_df.head())

print(funding_round_type_df.tail())
