# -*- coding: utf-8 -*-
"""
Proxy for total market capitalization at the company level

I follow the market cap procedure of Ferreira & Matos (2008)
in order to calculate a proxy for the total market cap at the company
level.

Input:
    own_sec_prices.parquet
    own_sec_universe.parquet
    sym_xc_isin.parquet
    
Output:
    mcap_total_company_level.parquet
    
"""

import os
import polars as pl


# ~~~~~~~~~~~~~~~~~~
#    DIRECTORIES 
# ~~~~~~~~~~~~~~~~~~

# Current directory
#cd = r'C:\Users\FMCC\Desktop\Ioannis'

# Parquet Factset tables
#factset_dir =  r'C:\FactSet_Downloadfiles\zips\parquet'
factset_dir = r'C:\Users\ropot\Desktop\Financial Data for Research\FactSet'


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


# ~~~~~~~~~~~~
#   IMPORT 
# ~~~~~~~~~~~~~

own_sec_prices = pl.read_parquet(os.path.join(factset_dir, 'own_sec_prices_eq.parquet'),
                                 use_pyarrow=True)

own_sec_ent = pl.read_parquet(os.path.join(factset_dir, 'own_sec_entity_eq.parquet'),
                              use_pyarrow=True)

own_sec_ent_hist = pl.read_parquet(os.path.join(factset_dir, 'own_sec_entity_hist_eq.parquet'),
                              use_pyarrow=True)

own_sec_cov = pl.read_parquet(os.path.join(factset_dir, 'own_sec_coverage_eq.parquet'),
                              use_pyarrow=True, columns=['FSYM_ID', 
                                                         'ISSUE_TYPE'])

sym_cov = pl.read_parquet(os.path.join(factset_dir, 'sym_coverage.parquet'),
                              use_pyarrow=True, columns=['FSYM_ID', 
                                                         'FREF_SECURITY_TYPE'])


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#   FORMAT OWN_SEC_PRICES TABLE
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


# Define quarter date 'date_q'
own_sec_prices = apply_quarter_scheme(own_sec_prices, 'PRICE_DATE')


# Keep only the most recent 'price' observation within a quarter
# for each security
prices_q = ( 
    own_sec_prices
    .group_by(['FSYM_ID', 'date_q'])
    .agg(pl.all().sort_by('PRICE_DATE').last())
    )


# Keep only positive unadjusted prices
prices_q = prices_q.filter(pl.col('UNADJ_PRICE')>0)

# Adjusted price of 0 is treated as null
prices_q = (
    prices_q.with_columns(
        pl.when(pl.col('ADJ_PRICE') == 0)
        .then(None)
        .otherwise(pl.col('ADJ_PRICE'))
        .alias('ADJ_PRICE')
        )
    )

# Define market cap
prices_q = prices_q.with_columns(
    (pl.col('UNADJ_PRICE')*pl.col('UNADJ_SHARES_OUTSTANDING')).alias('MCAP_FROM_UNADJ'),
    (pl.col('ADJ_PRICE')*pl.col('ADJ_SHARES_OUTSTANDING')).alias('MCAP_FROM_ADJ')
    )



# Keep market cap from unadjusted prices if it is not null and positive
prices_q = prices_q.with_columns(
    pl.when(pl.col('MCAP_FROM_UNADJ').is_not_null() & (pl.col('MCAP_FROM_UNADJ')>0))
    .then(pl.col('MCAP_FROM_UNADJ'))
    .otherwise(pl.col('MCAP_FROM_ADJ'))
    .alias('MCAP')
    )

# Define market cap dataframe and keep only positive market cap
mcap = (
        prices_q
        .select(['FSYM_ID', 'date_q', 'PRICE_DATE', 'MCAP'])
        .filter(pl.col('MCAP')>0)
        )


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#   AUGMENT MCAP WITH SECURITY INFORMATION
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Info from ownership bundle
mcap = mcap.join(own_sec_cov, on=['FSYM_ID'])
# Infor from symbology bundle
mcap = mcap.join(sym_cov, on=['FSYM_ID'])

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#       FILTER FOR NORMAL AND PREFERRED EQUITY 
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


# Total market cap has to be calculated from normal and preferred equity shares 
# as in Ferreira & Matos (2008). Note that ADR/GDRs  do not constitute additional 
# shares of a company.I


# Filter for normal equity 
is_eq = pl.col('ISSUE_TYPE') == 'EQ'
# Filter for preferred equity
is_prefeq = (pl.col('ISSUE_TYPE') == 'PF') & (pl.col('FREF_SECURITY_TYPE') == 'PREFEQ')
mcap_eq = mcap.filter(is_eq | is_prefeq)


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#   MAP SECURITY TO COMPANY
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Use symbology mappings 
own_sec_ent = own_sec_ent.rename({'FACTSET_ENTITY_ID' : 'FACTSET_ENTITY_ID_FROM_FSYM_ID'})
mcap_eq = mcap_eq.join(own_sec_ent, how='left', on=['FSYM_ID'])

# Use historical mappings if entities are missing. 
# But before using them format them

# Impute null 'end dates' with the most recent date of the dataset
end_date = mcap_eq['PRICE_DATE'].max()
own_sec_ent_hist = own_sec_ent_hist.with_columns(
    pl.when(pl.col('END_DATE').is_null())
    .then(end_date)
    .otherwise(pl.col('END_DATE'))
    .alias('END_DATE')
    )

# Rename 
own_sec_ent_hist = own_sec_ent_hist.rename({'FACTSET_ENTITY_ID' : 'FACTSET_ENTITY_ID_FROM_FSYM_ID_HIST'})


# Isolate the securities that did not match
fsym_id_not_matched = list(   
    mcap_eq
    .filter(pl.col('FACTSET_ENTITY_ID_FROM_FSYM_ID').is_null())
    .unique('FSYM_ID')['FSYM_ID']
    )

own_sec_ent_hist_ = own_sec_ent_hist.filter(pl.col('FSYM_ID').is_in(fsym_id_not_matched))


# Join
mcap_eq = mcap_eq.join(own_sec_ent_hist_, how='left', on=['FSYM_ID'] )

# Fill missing mappings in which 'PRICE_DATE' is between 'START_DATE' & 'END_DATE'
mcap_eq = mcap_eq.with_columns(
    pl.when((pl.col('START_DATE') <= pl.col('PRICE_DATE')) &
            (pl.col('PRICE_DATE') <= pl.col('END_DATE') ))
    .then(pl.col('FACTSET_ENTITY_ID_FROM_FSYM_ID_HIST'))
    .otherwise(pl.col('FACTSET_ENTITY_ID_FROM_FSYM_ID'))
    .alias('FACTSET_ENTITY_ID_FROM_FSYM_ID')
    )


# Select columns and keep unique rows
mcap_eq = (
       mcap_eq
       .filter(pl.col('FACTSET_ENTITY_ID_FROM_FSYM_ID').is_not_null())
       .select(['FSYM_ID',
                'date_q',
                'PRICE_DATE',
                'MCAP',
                'FACTSET_ENTITY_ID_FROM_FSYM_ID'])
       .unique()
       )


# Save
mcap_eq.write_parquet(os.path.join(factset_dir, 'mcap_security_level.parquet'))

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#   SUM MARKET CAP OF DIFFERENT SECURITIES FOR EACH ENTITY-QUARTER
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


# Sum 'MCAP' for each entity-quarter pair
# I record 83,163 unique entities/firms
mcap_total = (
    mcap_eq
    .group_by(['FACTSET_ENTITY_ID_FROM_FSYM_ID', 'date_q'])
    .agg(pl.col('MCAP').sum().alias('MCAP_TOTAL'))
    )

# Sort
mcap_total = mcap_total.sort(by=['FACTSET_ENTITY_ID_FROM_FSYM_ID', 'date_q'])



# ~~~~~~~~~~~~
#    SAVE
# ~~~~~~~~~~

mcap_total.write_parquet(os.path.join(factset_dir, 'mcap_total_company_level.parquet'))











