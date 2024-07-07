# -*- coding: utf-8 -*-
"""
Re-arrange mutual funds tables 

The Ferreira and Matos (2008) replication method requires that all mutual 
funds table be stored in one big table. However, mutual funds reports tables 
are so big that cannot be handled all at once on my machine as in
Ferreira & Matos (2008). To ease the computational burden of the mutual funds
calculation

i) I re-arrange the mutual funds tables into 17 tables by 10,000 funds 
in each table so I can apply the Ferreira & Matos (2008) methodology in each
re-arranged table separately.

ii) Filter securities as per Ferreira & Matos (2008)




Input:
    \own_fund_eq_v5_full\own_fund_detail_eq_1.parquet
    .
    .
    .
    \own_fund_eq_v5_full\own_fund_detail_eq_15.parquet

Output:
    \own_fund_eq_v5_full_split_by_fund\funds_table_1.parquet
    .
    .
    .
    \own_fund_eq_v5_full_split_by_fund\funds_table_18.parquet

"""


import os
import polars as pl

# ~~~~~~~~~~~~~~~~~~
#    DIRECTORIES 
# ~~~~~~~~~~~~~~~~~~

# Current directory
cd = r'C:\Users\FMCC\Desktop\Ioannis'

# Parquet Factset tables
factset_dir =  r'C:\FactSet_Downloadfiles\zips\parquet'

# 13F filings
own_inst_13f_dir = os.path.join(factset_dir, 'own_inst_eq_v5_full')

# Stakes based sources including UKSR, RNS, 13D/G's, proxies, etc.
own_stakes_dir = os.path.join(factset_dir, 'own_stakes_eq_v5_full')

# Sum of Fund holdings
own_funds_dir = os.path.join(factset_dir, 'own_fund_eq_v5_full')





# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#   APPLY QUARTER SCHEME
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~

def apply_quarter_scheme(df, date_col):
    
    # Col_names
    df_col_names = df.columns
    
    df = df.with_columns(
        pl.col(date_col).dt.strftime('%Y%m').alias('yyyymm').cast(pl.Int32),
        )
    
    df =  df.with_columns(
        (pl.col('yyyymm')% 100).alias('month'),
        (pl.col('yyyymm')/100).floor().cast(pl.Int32).alias('year')
        )
    # Define quarter 'date_q' in integer format
    df = df.with_columns(
        pl.when(pl.col('month')<=3)
        .then(3)
        .when((pl.col('month')>3) & (pl.col('month')<=6))
        .then(6)
        .when((pl.col('month')>6) & (pl.col('month')<=9))
        .then(9)
        .otherwise(12)
        .alias('month_q')
        )
    
    df =  df.with_columns(
        (pl.col('year')*100 + pl.col('month_q')).alias('date_q')
        ).select(df_col_names + ['date_q'])   
   
    return df



# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#  CREATE NEW MUTUAL FUNDS DIRECTORY 
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

funds_folder = 'own_fund_eq_v5_full_split_by_fund' 
funds_dir = os.path.join(factset_dir, funds_folder)
if funds_folder not in os.listdir(factset_dir):
    os.mkdir(funds_dir)
    
    
    
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#   CREATE own_basic TABLE FOR FILTERING
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


# Import own_sec_coverage table
own_sec_cov = pl.read_parquet(os.path.join(factset_dir, 'own_sec_coverage_eq.parquet'),
                              use_pyarrow=True)

# Import sym_coverage table
sym_cov = pl.read_parquet(os.path.join(factset_dir, 'sym_coverage.parquet'),
                              use_pyarrow=True,
                              columns=['FSYM_ID', 'FREF_SECURITY_TYPE'])



# Import own_sec_entity_eq table
own_sec_entity_eq = pl.read_parquet(os.path.join(factset_dir, 'own_sec_entity_eq.parquet'),
                                use_pyarrow=True)


# Prices
own_sec_prices = pl.read_parquet(os.path.join(factset_dir, 'own_sec_prices_eq.parquet'),
                                 use_pyarrow=True)


# Define quarter date 'date_q'
own_sec_prices = apply_quarter_scheme(own_sec_prices, 'PRICE_DATE')


# Keep only the most recent 'price' observation within a quarter
# for each security (data already sorted)
own_sec_prices_q = ( 
    own_sec_prices
    .group_by(['FSYM_ID', 'date_q'])
    .agg(pl.all().sort_by('PRICE_DATE').last())
    )


# termination_date TABLE (Termination quarter for each owneship security)
termination_date = ( 
    own_sec_prices
    .group_by('FSYM_ID')
    .agg(pl.col('date_q').max().alias('TERMINATION_DATE'))
    )



# Keep necessary columns
own_sec_cov_ = own_sec_cov.select(['FSYM_ID',
                                   'ISSUE_TYPE',
                                   'ISO_COUNTRY'])

# Join
equity_secs = own_sec_cov_.join(sym_cov, how='left', on=['FSYM_ID'])

# Equity or ADR
filter1 = pl.col('ISSUE_TYPE').is_in(['EQ', 'AD'])

# Preffered share
filter2 = pl.col('ISSUE_TYPE').is_in(['PF']) & pl.col('FREF_SECURITY_TYPE').is_in(['PREFEQ'])

# equity_secs TABLE (Universe of stocks)
equity_secs = equity_secs.filter(filter1 | filter2)

# own_basic TABLE (Universe of stocks plus information)
own_basic = ( 
    equity_secs
    .join(own_sec_entity_eq, how='inner', on=['FSYM_ID'])
    .join(termination_date, how='inner', on=['FSYM_ID'])
    )


# Free memory
del own_sec_cov, sym_cov, own_sec_entity_eq, own_sec_prices
del own_sec_prices_q, termination_date, equity_secs

# ~~~~~~~~~~~~~~~~~~~~~~~~~
#    SPLIT THE DATASET
# ~~~~~~~~~~~~~~~~~~~~~~~~~


# Universe of stocks
own_securities = ( 
    own_basic
    .select(['FSYM_ID'])
    .unique()
    )
# as a list
own_securities = list(own_securities['FSYM_ID'])


# Import own_ent_funds table 
own_ent_funds = pl.read_parquet(os.path.join(factset_dir, 'own_ent_funds.parquet'),
                                use_pyarrow=True)

# Isolate all the funds as a list
all_funds = list(own_ent_funds.unique(['FACTSET_FUND_ID'])['FACTSET_FUND_ID'])

# Groups of up to 10,000 funds
funds_groups = [all_funds[x:x+10000] for x in range(0, len(all_funds), 10000)]


# Iterate through the groups
for k, fund_group in enumerate(funds_groups):
    
    print('Fund group %d \n' % (k+1))
    
    # Define the Polars DataFrame
    funds_table = pl.DataFrame()

    # Iterate through the funds datasets
    for dataset in os.listdir(own_funds_dir):
        
        print('%s for fund group %d is processed \n' % (dataset, k+1))
        
        # Import sum of funds dataset
        own_fund = pl.read_parquet(os.path.join(own_funds_dir, dataset),
                                   use_pyarrow=True)
        
        # Filter for funds 
        own_fund_ = own_fund.filter(pl.col('FACTSET_FUND_ID').is_in(fund_group))
        
        # Filter for securities
        own_fund_ = own_fund_.filter(pl.col('FSYM_ID').is_in(own_securities))
        
        # Concat
        funds_table = pl.concat([funds_table, own_fund_])
        
    # Save
    funds_table.write_parquet(os.path.join(funds_dir, 'funds_table_%d.parquet' % (k+1)))
    



