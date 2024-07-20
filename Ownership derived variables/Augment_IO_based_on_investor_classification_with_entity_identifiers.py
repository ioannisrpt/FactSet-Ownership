# -*- coding: utf-8 -*-
"""
Augment IO dataset with entity identifiers

Input:
    IO_by_geographic_investment_and_active_share_bartram2015.parquet
    entity_identifiers.parquet
    Factset_CRSP_Link_Table_beta_202307.csv
    
Output:

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


    
# ~~~~~~~~~~~~~~~~~~~~~
#   IMPORT  DATA    #
# ~~~~~~~~~~~~~~~~~~~~~


# Company level IO
io_comp = pl.read_parquet(os.path.join(cd, 'IO_by_geographic_investment_and_active_share_bartram2015.parquet'))


# Entity identifiers
entity_identifiers = pl.read_parquet(os.path.join(cd, 'entity_identifiers.parquet'))

# Factset-CRSP link table
fc_link = pl.read_csv(os.path.join(cd, 'Factset_CRSP_Link_Table_beta_202307.csv'),
                      ignore_errors=True,
                      truncate_ragged_lines=True)

# ~~~~~~~~~~~~~~~~
#   AUGMENT WITH ISIN
# ~~~~~~~~~~~~~~~

io_comp = io_comp.join(entity_identifiers.select(['COMPANY_ID', 'ISIN', 'FSYM_ID', 'CUSIP']),
                       how='left', 
                       on=['COMPANY_ID'])

# Keep rows up to December 2023
io_comp = io_comp.filter(pl.col('date_q')<=202312)


# ~~~~~~~~~~~~~~~~~~~~~~~~~
#   AUGMENT WITH PERMNO/PERMCO
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


# Isolate -S securities in link table
fc_link_ = fc_link.filter(pl.col('fsym_id_kind')=='S')
# Drop rows with missing linking dates
fc_link_ = fc_link_.drop_nulls(['link_bdate', 'link_edate'])

# Format link table for matching
fc_link_ = (
    fc_link_
    .select(['fsym_id', 
             'PERMNO', 
             'PERMCO', 
             'link_bdate',
             'link_edate'])
    .rename({'fsym_id' : 'FSYM_ID'})
    .cast({'PERMNO' : pl.Int32, 
           'PERMCO' : pl.Int32,
           'link_bdate' : pl.Int32,
           'link_edate' : pl.Int32})
    )

# Define link_date as end of month of quarter 
io_comp = io_comp.with_columns(
    (pl.col('date_q')*100 + 29).cast(pl.Int32).alias('link_date')
    )
 

# Use FSYM_ID-link_date pairs from IO dataset as reference
pairs = io_comp.select(['FSYM_ID', 'link_date']).drop_nulls()

# Left merge with pairs
pairs = pairs.join(fc_link_, 
                   how='left',
                   on=['FSYM_ID'])
# Drop rows for which PERMNO is missing -> keep US pairs
us_pairs = pairs.filter(pl.col('PERMNO').is_not_null())

# link_date is within the range of start and end date
us_pairs_ = us_pairs.filter( (pl.col('link_bdate')<=pl.col('link_date')) &
                             (pl.col('link_date')<=pl.col('link_edate')) )


# Augment with IO dataset with matched PERMNO/PERMCO
io_comp_ = io_comp.join(us_pairs_.drop(['link_bdate', 'link_edate']), 
                        how='left',
                        on=['FSYM_ID', 'link_date'])


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#   ISIN OF PRIMARY SECURITY IS MISSING
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Drop companies for which no ISIN exists for primary security
io_comp_ = io_comp_.drop_nulls(['ISIN'])



# ~~~~~~~~~~~~~~~
#   SAVE
# ~~~~~~~~~~~

io_comp_ = io_comp_.drop(['link_date'])

io_comp_.write_parquet(os.path.join(cd, 'IO_by_geographic_investment_and_active_share_bartram2015_v2.parquet'))

# ~~~~~~~~~~~~~~~~
#   SANITY CHECKS
# ~~~~~~~~~~~~~~~~~~


num_firms = (
    io_comp_
    .group_by('date_q')
    .agg(pl.col('COMPANY_ID').count().alias('NUM_FIRMS'))
    .sort(by='date_q')
    )

num_firms.to_pandas().set_index('date_q').plot()







