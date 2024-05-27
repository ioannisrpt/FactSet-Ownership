# -*- coding: utf-8 -*-
"""
Do some sanity checkes
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


# ~~~~~~~~~~~~
#  IMPORT DATA
# ~~~~~~~~~~~~~

# Import factset holdings at the company level
fh_comp = pl.read_parquet(os.path.join(cd, 'factset_mcap_holdings_company_level.parquet'))

# Market cap at the company level
mcap_total = pl.read_parquet(os.path.join(cd, 'mcap_total_company_level.parquet'))

# ~~~~~~~~~~~~~~~~~~~~~~~
# COMPANY-LEVEL IO CHECK
# ~~~~~~~~~~~~~~~~~~~~~~~

# IO at the company-holder-quarter level
fh_comp = fh_comp.with_columns(
    (pl.col('MCAP_HELD') / pl.col('MCAP_TOTAL')).alias('IO')
    )

# IO at the company-quarter level
io_comp = (
      fh_comp
      .group_by(['FACTSET_ENTITY_ID_FROM_FSYM_ID', 'date_q'])
      .agg(pl.col('IO').sum())
      )

io_comp_ = io_comp.filter(pl.col('IO')<=1)


# MEAN IO 


mean_io = (
    io_comp_
    .group_by(['date_q'])
    .agg(pl.col('IO').mean())
    .sort(by='date_q')
    )


mean_io.to_pandas().set_index('date_q').plot()

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#    AGGREGATED INSTITUTIONAL HOLDINGS
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# -----------------------------
#   Control for IO more than 1
# -----------------------------


# Keep only the companies/quarters that have IO less than 1.
fh_comp_ = fh_comp.join(io_comp_.select(['FACTSET_ENTITY_ID_FROM_FSYM_ID', 'date_q']),
                        how='inner',
                        on =['FACTSET_ENTITY_ID_FROM_FSYM_ID', 'date_q'])



# Sum of all holdings 
mcap_held = (
    fh_comp_
    .group_by('date_q')
    .agg(pl.col('MCAP_HELD').sum())
    .sort(by='date_q')
    )

# Sum of market cap of securitites being owned
mcap_sum = (
    mcap_total
    .group_by('date_q')
    .agg(pl.col('MCAP_TOTAL').sum())
    .sort(by='date_q')
    )

# Inner merge
io_agg = mcap_held.join(mcap_sum, on='date_q')

io_agg = io_agg.with_columns(    
    (pl.col('MCAP_HELD')/pl.col('MCAP_TOTAL')).alias('IO')
    )


# Plot
io_agg.select(['date_q', 'IO']).to_pandas().set_index('date_q').plot()