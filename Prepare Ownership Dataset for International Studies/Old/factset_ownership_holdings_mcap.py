# -*- coding: utf-8 -*-
"""
Augment factset ownership holdings data with total number of shares outstanding
and adjusted prices. All prices are displayed in USD.


Factset ownership holdings data (factset_holdings.parquet is the complete dataset)
contains adjusted number of shares for a security that an entity/holder holds.
I need to convert the adjusted number of shares to the market capitalization (in USD)
held by entities and augment the dataset with the total market capitalization of
the security (-S). This will solve problems when calculating ownership variables
involving ADR/GDR shares or any other instrument that is not 1-to-1 ratio to
the ordinary share of a firm. 

Keep only observations that have non-null adjusted price and number of shares
outstanding. 

I also assume (without checking) that the own_sec_prices table contains 
the adjusted price of an ADR/GDR and so on along with the ordinary share
listing. 


Input:
    factset_holdings.parquet
    own_sec_prices.parquet
    
Output:
    factset_holdings_mcap.parquet
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

"""
fh = pl.read_parquet(os.path.join(cd, 'factset_holdings.parquet'),
                              use_pyarrow=True)
"""

fh = pl.read_parquet(os.path.join(factset_dir, 'factset_holdings.parquet'),
                              use_pyarrow=True)


own_sec_prices = pl.read_parquet(os.path.join(factset_dir, 'own_sec_prices_eq.parquet'),
                                 use_pyarrow=True)

own_sec_ent = pl.read_parquet(os.path.join(factset_dir, 'own_sec_entity_eq.parquet'),
                              use_pyarrow=True)

own_sec_cov = pl.read_parquet(os.path.join(factset_dir, 'own_sec_coverage_eq.parquet'),
                              use_pyarrow=True, columns=['FSYM_ID', 
                                                         'ISSUE_TYPE'])

sym_cov = pl.read_parquet(os.path.join(factset_dir, 'sym_coverage.parquet'),
                              use_pyarrow=True, columns=['FSYM_ID', 
                                                         'FREF_SECURITY_TYPE'])


# ~~~~~~~~~~~~
#   FORMAT 
# ~~~~~~~~~~~


# Define quarter date 'date_q'
own_sec_prices = apply_quarter_scheme(own_sec_prices, 'PRICE_DATE')


# Keep only the most recent 'price' observation within a quarter
# for each security
prices_q = ( 
    own_sec_prices
    .group_by(['FSYM_ID', 'date_q'])
    .agg(pl.all().sort_by('PRICE_DATE').last())
    )



# ~~~~~~~~~~~~~
#   AUGMENT WITH ADJUSTED PRICE -->  MARKET CAP HELD
# ~~~~~~~~~~~~~


# Augment holdings with adjusted price
adj_prices_q = ( prices_q
             .select(['FSYM_ID', 
                      'date_q', 
                      'ADJ_PRICE'])
             )

# Inner join takes care of non-null prices and total number of shares
fh_ = fh.join(adj_prices_q, how='inner', on=['FSYM_ID', 'date_q'])

# Calculate holding cap (in USD)
fh_ = fh_.with_columns(
    (pl.col('ADJ_SHARES_HELD') * pl.col('ADJ_PRICE')).alias('MCAP_HELD')
    )


# Keep only necessary columns
fh_ = fh_.select(['FSYM_ID', 
                  'FACTSET_ENTITY_ID', 
                  'date_q',
                  'SCHEME',
                  'MCAP_HELD'])


# Augment holdings data with entity id (-E) for each security 
fh_ = fh_.join(own_sec_ent.rename({'FACTSET_ENTITY_ID' : 'FACTSET_ENTITY_ID_FSYM_ID'}),
               how='left', on = ['FSYM_ID'])



# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#   AUGMENT WITH TOTAL MARKET CAP 
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


# Map securities to entities/firms 
# Every security(-S) is mapped to an entityt/firm (-E)
prices_q = prices_q.join(own_sec_ent, on=['FSYM_ID'])

# Calculate market cap for each security using unadjusted numbers
mcap = prices_q.with_columns(
    (pl.col('UNADJ_PRICE') * pl.col('UNADJ_SHARES_OUTSTANDING')).alias('MCAP')
    )
mcap = mcap.select(['FSYM_ID', 'date_q', 'FACTSET_ENTITY_ID', 'MCAP'])

# mcap.filter(pl.col('MCAP')>0).shape[0] / mcap.shape[0]
# 68.5%


# Total market cap has to be calculated from normal and preferred equity shares 
# as ADR/GDRs are a subset of those (ADR/GDRs do not constitute additional shares
# of the same firm) I use Ferreira & Matos (2008) methodology to proxy for the market cap
# of a company.

mcap = mcap.join(own_sec_cov, on = ['FSYM_ID'])
mcap = mcap.join(sym_cov, on = ['FSYM_ID'])

# Filter for normal equity 
is_eq = pl.col('ISSUE_TYPE') == 'EQ'
# Filter for preferred equity
is_prefeq = (pl.col('ISSUE_TYPE') == 'PF') & (pl.col('FREF_SECURITY_TYPE') == 'PREFEQ')
mcap_ = mcap.filter(is_eq | is_prefeq)

# mcap_.filter(pl.col('MCAP')>0).shape[0] / mcap_.shape[0]
# 97.8%

# Sum 'mcap' for each entity-quarter pair
# I record 84,217 unique entities/firms
mcap_total = (
    mcap_
    .group_by(['FACTSET_ENTITY_ID', 'date_q'])
    .agg(pl.col('MCAP').sum().alias('MCAP_TOTAL'))
    )

# mcap_total.filter(pl.col('MCAP_TOTAL')>0).shape[0] / mcap_total.shape[0]
# 97.9%

# Non-zero total market cap
# I record 83,162 unique entities/firms for which market cap is positive
mcap_total = mcap_total.filter(pl.col('MCAP_TOTAL')>0)

# 'FACTSET_ENTITY_ID' field name corresponds to the institution
# for holding data thus I need another name for the entity that 
# issues the security: 'FACTSET_ENTITY_ID_FSYM_ID'
mcap_total = mcap_total.rename({'FACTSET_ENTITY_ID' : 'FACTSET_ENTITY_ID_FSYM_ID'})


# Augment holdings with positive total market capitalization of a firm
# that has issued ordinary and/or preferred equity shares 
fh_ = fh_.join(mcap_total, how='left', on = ['FACTSET_ENTITY_ID_FSYM_ID', 'date_q'])

# ~~~~~~~~~~~~~~~
#    SAVE
# ~~~~~~~~~~~~~

#fh_.write_parquet(os.path.join(cd, 'factset_holdings_mcap.parquet'))

fh_.write_parquet(os.path.join(factset_dir, 'factset_holdings_mcap.parquet'))









