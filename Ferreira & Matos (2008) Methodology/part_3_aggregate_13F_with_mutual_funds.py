# -*- coding: utf-8 -*-
"""
Replication of Ferreira & Matos (2008) methodology

PART 3 - AGGREGATE 13F WITH MUTUAL FUNDS DATA

When both 13F reports and fund reports are available
 for a institution-security-quarter observation, 
 use 13F for US securities,
 use the maximum holding of 13F and fund reports for non-US securities.

Market cap is in millions of USD.

Input:
    v2_holdings13f.parquet
    v2_holdingsmf.parquet
    hmktcap.parquet

    
Output:
    holdingsall_company_level.parquet
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

# Mutual fund holdings
funds_dir = os.path.join(factset_dir, 'own_fund_eq_v5_full_split_by_fund')




# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#        IMPORT DATA
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Holdings from 13F reports
v2_holdings13f = pl.read_parquet(os.path.join(cd, 'v2_holdings13f.parquet'),
                                 use_pyarrow=True,
                                 columns = ['FACTSET_ENTITY_ID',
                                            'FSYM_ID',
                                            'date_q',
                                            'IO',
                                            'COMPANY_ID',
                                            'ISO_COUNTRY'])
v2_holdings13f = v2_holdings13f.rename({'ISO_COUNTRY' : 'SEC_COUNTRY'})


# Holdings from Mutual Funds reports
v2_holdingsmf = pl.read_parquet(os.path.join(cd, 'v2_holdingsmf.parquet'),
                                 use_pyarrow=True,
                                 columns = ['FACTSET_ENTITY_ID',
                                            'FSYM_ID',
                                            'date_q',
                                            'IO',
                                            'COMPANY_ID',
                                            'ISO_COUNTRY'])
v2_holdingsmf = v2_holdingsmf.rename({'ISO_COUNTRY' : 'SEC_COUNTRY'})


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#    INSTITUTION-QUARTER PAIRS IN 13F AND MF
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


# inst_quarter_13f TABLE: institution-quarter pairs from 13F data
inst_quarter_13f =  ( 
    v2_holdings13f
    .select(['FACTSET_ENTITY_ID', 'date_q'])
    .unique()
    )

# inst_quarter_mf TABLE: institution-quarter pairs from MF data
inst_quarter_mf = ( 
    v2_holdingsmf
    .select(['FACTSET_ENTITY_ID', 'date_q'])
    .unique()
    )



# Create dummy variables is_in_13f = 1 and is_in_mf if an institution
# appears in 13F and MF data, respectively
inst_quarter_13f = inst_quarter_13f.rename({'FACTSET_ENTITY_ID' : 'FACTSET_ENTITY_ID_13F',
                                            'date_q': 'date_q_13F'})
inst_quarter_mf = inst_quarter_mf.rename({'FACTSET_ENTITY_ID' : 'FACTSET_ENTITY_ID_MF',
                                          'date_q' : 'date_q_MF'})

inst_quarter = inst_quarter_13f.join(inst_quarter_mf, 
                                     how='full',
                                     left_on = ['FACTSET_ENTITY_ID_13F', 'date_q_13F'],
                                     right_on = ['FACTSET_ENTITY_ID_MF', 'date_q_MF']) 

inst_quarter = ( 
    inst_quarter.with_columns(
        pl.when(pl.col('FACTSET_ENTITY_ID_13F').is_not_null())
        .then(pl.col('FACTSET_ENTITY_ID_13F'))
        .otherwise(pl.col('FACTSET_ENTITY_ID_MF'))
        .alias('FACTSET_ENTITY_ID'),
        pl.when(pl.col('date_q_13F').is_not_null())
        .then(pl.col('date_q_13F'))
        .otherwise(pl.col('date_q_MF'))
        .alias('date_q')
        )
    )

inst_quarter = inst_quarter.sort(by=['FACTSET_ENTITY_ID', 'date_q'])

inst_quarter = (
    inst_quarter.with_columns(
        pl.when(pl.col('FACTSET_ENTITY_ID_13F').is_not_null())
        .then(1)
        .otherwise(0)
        .alias('IS_IN_13F'),
        pl.when(pl.col('FACTSET_ENTITY_ID_MF').is_not_null())
        .then(1)
        .otherwise(0)
        .alias('IS_IN_MF')
        )
    )

inst_quarter = inst_quarter.select(['FACTSET_ENTITY_ID',
                                    'date_q',
                                    'IS_IN_13F',
                                    'IS_IN_MF'])

# Free memory
del inst_quarter_13f, inst_quarter_mf

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#   AGGREGATE 13F WITH MUTUAL FUNDS DATA
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Define US securities
v2_holdings13f = v2_holdings13f.with_columns(
    pl.when(pl.col('SEC_COUNTRY') == 'US')
    .then(1)
    .otherwise(0)
    .alias('IS_IN_US')
    )

v2_holdingsmf = v2_holdingsmf.with_columns(
    pl.when(pl.col('SEC_COUNTRY') == 'US')
    .then(1)
    .otherwise(0)
    .alias('IS_IN_US')
    )


# Augment with institution classification
v2_holdings13f = v2_holdings13f.join(inst_quarter, 
                                     how='left',
                                     on=['FACTSET_ENTITY_ID', 'date_q'])

v2_holdingsmf = v2_holdingsmf.join(inst_quarter, 
                                     how='left',
                                     on=['FACTSET_ENTITY_ID', 'date_q'])

# Holdings from institutions that appear only in 13F reports 
holdings13f_only = ( 
    v2_holdings13f.filter((pl.col('IS_IN_13F') == 1) & (pl.col('IS_IN_MF')==0) )
    )

# Holdings from institutions that appear only in MF reports
holdingsmf_only = ( 
    v2_holdingsmf.filter((pl.col('IS_IN_13F') == 0) & (pl.col('IS_IN_MF')==1) )
    )

# Holdings from institutions that appear in both 13F and MF reports
holdings13f_both  = ( 
    v2_holdings13f.filter((pl.col('IS_IN_13F') == 1) & (pl.col('IS_IN_MF')==1) )
    )

holdingsmf_both = ( 
    v2_holdingsmf.filter((pl.col('IS_IN_13F') == 1) & (pl.col('IS_IN_MF')==1) )
    )

holdings13fmf = holdings13f_both.join(holdingsmf_both,
                                      on=['FACTSET_ENTITY_ID', 'FSYM_ID', 'date_q'],
                                      how='full',
                                      coalesce = True
                                      )

f = (pl.col('FACTSET_ENTITY_ID') == '000BJX-E') & (pl.col('FSYM_ID')=='B04T5J-S') & (pl.col('date_q')==200212)


# Coalesce company and institution information
holdings13fmf = ( 
    holdings13fmf.with_columns(
        pl.coalesce(['COMPANY_ID', 'COMPANY_ID_right']).alias('COMPANY_ID'),
        pl.coalesce(['SEC_COUNTRY', 'SEC_COUNTRY_right']).alias('SEC_COUNTRY'),
        pl.coalesce(['IS_IN_US', 'IS_IN_US_right']).alias('IS_IN_US'),
        pl.coalesce(['IS_IN_13F', 'IS_IN_13F_right']).alias('IS_IN_13F'),
        pl.coalesce(['IS_IN_MF', 'IS_IN_MF_right']).alias('IS_IN_MF')
        )
    .drop(['COMPANY_ID_right', 
           'SEC_COUNTRY_right',
           'IS_IN_US_right',
           'IS_IN_13F_right',
           'IS_IN_MF_right'])
    .rename({'IO' : 'IO_13F', 'IO_right' : 'IO_MF'})
    )

# Free memory
del v2_holdings13f, v2_holdingsmf


# Institution-security-quarter IO
holdings13fmf = holdings13fmf.with_columns(
    pl.when(pl.col('IO_13F').is_not_null() & pl.col('IO_MF').is_null() )
    .then(pl.col('IO_13F'))
    .when(pl.col('IO_13F').is_null() & pl.col('IO_MF').is_not_null())
    .then(pl.col('IO_MF'))
    .when(pl.col('IO_13F').is_not_null() & pl.col('IO_MF').is_not_null() & (pl.col('IS_IN_US')==1))
    .then(pl.col('IO_13F'))
    .when(pl.col('IO_13F').is_not_null() & pl.col('IO_MF').is_not_null() & (pl.col('IS_IN_US')==0))
    .then(pl.max_horizontal('IO_13F', 'IO_MF'))
    .alias('IO')
    )

holdings13fmf_ = holdings13fmf.select(['FACTSET_ENTITY_ID',
                                      'FSYM_ID',
                                      'date_q',
                                      'IO',
                                      'COMPANY_ID',
                                      'SEC_COUNTRY'])


# Keep necessary columns
holdings13f_only = holdings13f_only.select(['FACTSET_ENTITY_ID',
                                      'FSYM_ID',
                                      'date_q',
                                      'IO',
                                      'COMPANY_ID',
                                      'SEC_COUNTRY'])

holdingsmf_only = holdingsmf_only.select(['FACTSET_ENTITY_ID',
                                      'FSYM_ID',
                                      'date_q',
                                      'IO',
                                      'COMPANY_ID',
                                      'SEC_COUNTRY'])

# v1_holdingsall TABLE: aggregated 13F and MF ownership data 
v1_holdingsall = pl.concat([holdings13f_only,
                            holdingsmf_only, 
                            holdings13fmf_])

# Sort
v1_holdingsall = v1_holdingsall.sort(by=['FACTSET_ENTITY_ID', 'FSYM_ID', 'date_q'])

# Free memory
del holdings13f_only, holdingsmf_only, holdings13fmf, holdings13fmf_


# ~~~~~~~~~~~~~~~~~~~~~~~~~~
#   ADJUST IO WHEN IO>1 
# ~~~~~~~~~~~~~~~~~~~~~~~~~~


# adjfactor TABLE: adjustment factor for IO ratios greater than 1 at the 
# company level
adjfactor = (
    v1_holdingsall
    .group_by(['COMPANY_ID', 'date_q'])
    .agg(pl.col('IO').sum())
    )

adjfactor = adjfactor.with_columns(
    pl.when(pl.col('IO')>1)
    .then(pl.col('IO'))
    .otherwise(1)
    .alias('adjf')  
    )

adjfactor = ( 
    adjfactor
    .select(['COMPANY_ID', 'date_q', 'adjf'])
    .sort(by=['COMPANY_ID', 'date_q'])
    )

# Augment v1_holdingsall with adjustment factor
v1_holdingsall = v1_holdingsall.join(adjfactor,
                                     on=['COMPANY_ID', 'date_q'])

# Adjust IO at the security level
v1_holdingsall = ( 
    v1_holdingsall.with_columns(
    (pl.col('IO')/pl.col('adjf')).alias('IO')
    )
    .drop(['adjf'])
    )


# Free memory
del adjfactor

# ~~~~~~~~~~~~~~~~~~~~~~~~
#   COMPANY MARKET CAP 
# ~~~~~~~~~~~~~~~~~~~~~~~~

# Import company-level market capitalization in millions USD
hmktcap = pl.read_parquet(os.path.join(cd, 'hmktcap.parquet'),
                          use_pyarrow=True)
hmktcap = hmktcap.rename({'FACTSET_ENTITY_ID' : 'COMPANY_ID'})

# Augment holdings with company market cap
v2_holdingsall = v1_holdingsall.join(hmktcap, 
                                     how='left',
                                     on=['COMPANY_ID', 'date_q'])

# Market cap holdings
v2_holdingsall = v2_holdingsall.with_columns(
    (pl.col('IO')*pl.col('MKTCAP_USD')).alias('MKTCAP_HELD')
    )


# ~~~~~~~~~~~~~~~
#     SAVE
# ~~~~~~~~~~~~~~~~

v2_holdingsall.write_parquet(os.path.join(cd, 'holdingsall_company_level.parquet'))



# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#  SANITY CHECK WITH AGGREGATE INSTITUTIONAL HOLDINGS
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Sum of all institutional holdings 
mcap_held = (
    v2_holdingsall
    .group_by('date_q')
    .agg(pl.col('MKTCAP_HELD').sum())
    .sort(by='date_q')
    )

# Sum of market cap of securitites being owned
mcap_sum = (
    hmktcap
    .group_by('date_q')
    .agg(pl.col('MKTCAP_USD').sum())
    .sort(by='date_q')
    )

# Inner merge
io_agg = mcap_held.join(mcap_sum, on='date_q')

io_agg = io_agg.with_columns(    
    (pl.col('MKTCAP_HELD')/pl.col('MKTCAP_USD')).alias('IO')
    )


# Plot
io_agg.select(['date_q', 'IO']).to_pandas().set_index('date_q').plot()










    




