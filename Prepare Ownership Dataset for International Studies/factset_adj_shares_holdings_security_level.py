# -*- coding: utf-8 -*-
"""
Prepare institutional ownership adjusted shares holdings at the security level.

Filter for only ordinary and preferred equity shares including GDR/ADRs. 


Notes:
    1. There are cases when adjusted shares outstanding are missing.
    
    2. There are cases when adjusted shares oustanding are zero. 

    

Input:
    factset_adj_shares_holdings.parquet
    own_sec_prices_q.parquet
    own_sec_universe.parquet
    sym_xc_isin.parquet
    
Output:
    factset_adj_shares_holdings_security_level.parquet
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
#factset_dir = r'C:\Users\ropot\Desktop\Financial Data for Research\FactSet'


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
#  IMPORT DATA
# ~~~~~~~~~~~~~

# Ownership market cap holdings at the security level
"""
fh = pl.read_parquet(os.path.join(factset_dir, 'factset_mcap_holdings.parquet'),
                              use_pyarrow=True)
"""

fh = pl.read_parquet(os.path.join(cd, 'factset_adj_shares_holdings.parquet'),
                              use_pyarrow=True)

# Full universe of securities covered in ownership bundle along with additional info
"""
own_sec_uni = pl.read_parquet(os.path.join(factset_dir, 'own_sec_universe.parquet'),
                                 use_pyarrow=True)
"""

own_sec_uni = pl.read_parquet(os.path.join(cd, 'own_sec_universe.parquet'),
                                 use_pyarrow=True)

# The field 'factset_entity_id' except for holdings table 
# corresponds to the entity that issues the security 
# (and not to the institution that holds it as it is in the holdings table)
own_sec_uni = own_sec_uni.rename({'FACTSET_ENTITY_ID' : 'FACTSET_ENTITY_ID_FROM_FSYM_ID'})

# Current ISINs
sym_isin = pl.read_parquet(os.path.join(factset_dir, 'sym_xc_isin.parquet'))

# Import own_sec_prices
own_sec_prices = pl.read_parquet(os.path.join(factset_dir, 'own_sec_prices_eq.parquet'),
                                 use_pyarrow=True)



# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#  FORMAT OWN_SEC_PRICES TABLE
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


# Define quarter date 'date_q'
own_sec_prices = apply_quarter_scheme(own_sec_prices, 'PRICE_DATE')


# Keep only the most recent 'price' observation within a quarter
# for each security (data already sorted)
own_sec_prices_q = ( 
    own_sec_prices
    .group_by(['FSYM_ID', 'date_q'])
    .agg(pl.all().sort_by('PRICE_DATE').last())
    )


# Adjusted positions only
adj_q = (own_sec_prices_q
            .select(['FSYM_ID',
                     'date_q',
                     'ADJ_PRICE',
                     'ADJ_SHARES_OUTSTANDING'])
            )

# Keep only positive adjusted prices
adj_q = adj_q.filter(pl.col('ADJ_PRICE')>0)


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#  ORDINARY OR PREFERRED EQUITY SHARES + ADR/GDR - COMPANIES THAT ISSUED THEM
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# I follow Ferreira & Matos (2008) in defining my universe of securities

# Filter for normal equity (ordinary equity + ADR/GDR)
is_eq_and_ad = pl.col('ISSUE_TYPE').is_in(['EQ', 'AD'])
# Filter for preferred equity 
is_prefeq = (pl.col('ISSUE_TYPE') == 'PF') & (pl.col('FREF_SECURITY_TYPE') == 'PREFEQ')

# Universe of stocks 
own_securities = own_sec_uni.filter(is_eq_and_ad | is_prefeq)
# Universe of entities/companies
own_entities = list(own_securities.unique('FACTSET_ENTITY_ID_FROM_FSYM_ID')['FACTSET_ENTITY_ID_FROM_FSYM_ID'])

print('There are %d unique entities/companies in our universe.\n' % len(own_entities))


# For each company, isolate the primary listing of its security and its ISO country 
plisting = own_securities.unique(['FACTSET_ENTITY_ID_FROM_FSYM_ID', 'FSYM_PRIMARY_EQUITY_ID'])
plisting_ = (
                plisting
                .select(['FACTSET_ENTITY_ID_FROM_FSYM_ID', 
                        'FSYM_PRIMARY_EQUITY_ID',                            
                        'ISO_COUNTRY_ENTITY'])
                .drop_nulls(['FACTSET_ENTITY_ID_FROM_FSYM_ID'])
                )

r"""
# Add ISIN code for the primary listing of a security
# The ISIN for primary listing (-S) is missing for 1,180 companies
plisting_ = plisting_.join(sym_isin.rename({'FSYM_ID' : 'FSYM_PRIMARY_EQUITY_ID'}),
                           how='left',
                           on=['FSYM_PRIMARY_EQUITY_ID'])
"""




# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#  KEEP ONLY ENTITIES THAT HAVE ISSUED ORDINARY OR PREFERRED EQUITY SHARES + ADR/GDR
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Universe of securities as a list
own_securities_ls = list(own_securities.unique(['FSYM_ID'])['FSYM_ID'])
# There are 107,264 securities in total

# Filter adjusted shares holdings
fh_ = fh.filter(pl.col('FSYM_ID').is_in(set(own_securities_ls)))
# there are 88_958_368 rows




# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#   HOLDINGS AT THE SECURITY LEVEL 
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Augment with adjusted shares outstanding 
adj_q_out = adj_q.select(['FSYM_ID', 
                          'date_q',
                          'ADJ_SHARES_OUTSTANDING'])

fh_sec = fh_.join(adj_q_out, 
                how='left', 
                on = ['FSYM_ID', 'date_q'])


# -----------------
# SOME HOUSEKEEPING
# -----------------

# I have null total and holdings market cap values
fh_sec = fh_sec.drop_nulls()

# Only positive market cap
fh_sec = fh_sec.filter((pl.col('MCAP_TOTAL')>0) &
                         (pl.col('MCAP_HELD')>0)  )


# If holdings exceed market cap value at the security level, then
# the holdings are deemed invalid and dropped.
fh_sec = fh_sec.with_columns(
    (pl.col('MCAP_HELD') / pl.col('MCAP_TOTAL')).alias('IO')
    )

fh_sec = fh_sec.filter(pl.col('IO') <= 1)



# Final form of dataset
fh_sec = (
    fh_sec
    .select(['FSYM_ID',
             'FACTSET_ENTITY_ID',
             'date_q',
             'MCAP_HELD',
             'MCAP_TOTAL',
             'FACTSET_ENTITY_ID_FROM_FSYM_ID',
             'SCHEME'])
    .sort(by=['FSYM_ID',
              'FACTSET_ENTITY_ID',
              'date_q'])
    )

# ~~~~~~~
#  SAVE 
# ~~~~~~~

#fh_sec.write_parquet(os.path.join(factset_dir, 'factset_mcap_holdings_company_level.parquet'))

fh_sec.write_parquet(os.path.join(cd, 'factset_mcap_holdings_security_level.parquet'))













