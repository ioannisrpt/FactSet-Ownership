# -*- coding: utf-8 -*-
"""
Aggregate institutional ownership market cap holdings at the company level.

A firm might have issued multiple securities that are in turn owned by
different or the same institutions. Thus market holdings have to be aggregated
at the company rather than the security level.

Augment the dataset with the total market cap of the company for
IO calculations.

Input:
    factset_mcap_holdings.parquet
    mcap_total_company_leve.parquet
    own_sec_universe.parquet
    sym_xc_isin.parquet
    
Output:
    factset_mcap_holdings_company_level.parquet
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


# ~~~~~~~~~~~~
#  IMPORT DATA
# ~~~~~~~~~~~~~

# Ownership market cap holdings at the security level
fh = pl.read_parquet(os.path.join(factset_dir, 'factset_mcap_holdings.parquet'),
                              use_pyarrow=True)

# Total market cap at the company level
mcap_total = pl.read_parquet(os.path.join(factset_dir, 'mcap_total_company_level.parquet'),
                              use_pyarrow=True) 

# Full universe of securities covered in ownership bundle along with additional info
own_sec_uni = pl.read_parquet(os.path.join(factset_dir, 'own_sec_universe.parquet'),
                                 use_pyarrow=True)

# Current ISINs
sym_isin = pl.read_parquet(os.path.join(factset_dir, 'sym_xc_isin.parquet'))



# The field 'factset_entity_id' except for holdings table 
# corresponds to the entity that issues the security 
# (and not to the institution that holds it as it is in the holdings table)
own_sec_uni = own_sec_uni.rename({'FACTSET_ENTITY_ID' : 'FACTSET_ENTITY_ID_FROM_FSYM_ID'})


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

# Filter market cap holdings
fh_ = fh.filter(pl.col('FSYM_ID').is_in(set(own_securities_ls)))
# there are 88_860_326 rows




# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#  AGGREGATE HOLDINGS OVER COMPANY-QUARTER-ENTITY/INSTITUION
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Augment with company identifier (-E)
security_company_map = own_securities.select(['FSYM_ID',
                                              'FACTSET_ENTITY_ID_FROM_FSYM_ID'])
fh_ = fh_.join(security_company_map,
               how='left',
               on=['FSYM_ID'])

# Mapping is not perfect and I lose market cap holdings. How do I fix this?
# Use historical mappings of securities to companies?

# Total holdings of a company for an entity/institution - quarter pair
fh_company = (
    fh_
    .group_by(['FACTSET_ENTITY_ID_FROM_FSYM_ID', 'FACTSET_ENTITY_ID', 'date_q'])
    .agg(pl.col('MCAP_HELD').sum())
    )

fh_comp = fh_company.drop_nulls()

# Augment again with company market capitalization
fh_comp = fh_comp.join(mcap_total, how='left', on = ['FACTSET_ENTITY_ID_FROM_FSYM_ID', 'date_q'])

# Augment with primary equity listing identifier (-S) and ISO country
# for issuer entity/company
#fh_company = fh_company.join(plisting_, how='left', on =['FACTSET_ENTITY_ID_FSYM_ID'])

# Free memory
del fh_, plisting, plisting_


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#      SOME HOUSEKEEPING
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# I have null total and holdings market cap values
fh_comp = fh_comp.drop_nulls()

# Only positive market cap
fh_comp = fh_comp.filter((pl.col('MCAP_TOTAL')>0) &
                         (pl.col('MCAP_HELD')>0)  )



# ~~~~~~~
# SAVE 
# ~~~~~~~

fh_comp.write_parquet(os.path.join(factset_dir, 'factset_mcap_holdings_company_level.parquet'))


# COMPANY-LEVEL IO CHECK

f = fh_comp.with_columns(
    (pl.col('MCAP_HELD') / pl.col('MCAP_TOTAL')).alias('IO')
    )


io_comp = (
      f
      .group_by(['FACTSET_ENTITY_ID_FROM_FSYM_ID', 'date_q'])
      .agg(pl.col('IO').sum())
      )

io_comp_ = io_comp.filter(pl.col('IO')<=1)

mean_io = (
    io_comp_
    .group_by(['date_q'])
    .agg(pl.col('IO').mean())
    .sort(by='date_q')
    )


mean_io.to_pandas().set_index('date_q').plot()


# AGGREGATED INSTITUTIONAL HOLDINGS FOR ALL STOCKS

# Sum of all holdings 
mcap_held = (
    fh_comp
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

io_agg = mcap_held.join(mcap_sum, on='date_q')

io_agg = io_agg.with_columns(    
    (pl.col('MCAP_HELD')/pl.col('MCAP_TOTAL')).alias('IO')
    )


# Plot
io_agg.select(['date_q', 'IO']).to_pandas().set_index('date_q').plot()


# AGGREGATED INSTITUTIONAL HOLDINGS FOR US STOCKS

mcap_held_us = (
    fh_comp
    .filter(pl.col('ISO_COUNTRY_ENTITY')=='US')
    .group_by('date_q')
    .agg(pl.col('MCAP_HELD').sum())
    .sort(by='date_q')   
    )


iso_country = (
    own_sec_uni
    .unique(['FACTSET_ENTITY_ID_FSYM_ID'])
    .select(['FACTSET_ENTITY_ID_FSYM_ID', 'ISO_COUNTRY_ENTITY'])
    )

mcap_total = mcap_total.join(iso_country,
                             how='left', 
                             on=['FACTSET_ENTITY_ID_FSYM_ID'])

mcap_sum_us = (
    mcap_total
    .filter(pl.col('ISO_COUNTRY_ENTITY')=='US')
    .group_by('date_q')
    .agg(pl.col('MCAP_TOTAL').sum())
    .sort(by='date_q') 
    )



io_agg_us = mcap_held_us.join(mcap_sum_us, on='date_q')

io_agg_us = io_agg_us.with_columns(    
    (pl.col('MCAP_HELD')/pl.col('MCAP_TOTAL')).alias('IO')
    )


# Plot
io_agg_us.select(['date_q', 'IO']).to_pandas().set_index('date_q').plot()






