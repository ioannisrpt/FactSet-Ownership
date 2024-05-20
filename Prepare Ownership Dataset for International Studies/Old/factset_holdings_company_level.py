# -*- coding: utf-8 -*-
"""
Aggregate institutional ownership holdings at the company level.



Input:
    factset_holdings_mcap.parquet
    own_sec_universe.parquet
    sym_xc_isin.parquet
    
Output:
    factset_holdings_company_level.parquet
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

"""
fh = pl.read_parquet(os.path.join(cd, 'factset_holdings_mcap.parquet'),
                              use_pyarrow=True)

own_sec_uni = pl.read_parquet(os.path.join(cd, 'own_sec_universe.parquet'),
                                 use_pyarrow=True)

"""

fh = pl.read_parquet(os.path.join(factset_dir, 'factset_holdings_mcap.parquet'),
                              use_pyarrow=True)


own_sec_uni = pl.read_parquet(os.path.join(factset_dir, 'own_sec_universe.parquet'),
                                 use_pyarrow=True)

sym_isin = pl.read_parquet(os.path.join(factset_dir, 'sym_xc_isin.parquet'))



# The field 'factset_entity_id' except for holdings table 
# corresponds to the entity that issues the security 
# (and not to the institution that holds it as it is in the holdings table)
own_sec_uni = own_sec_uni.rename({'FACTSET_ENTITY_ID' : 'FACTSET_ENTITY_ID_FSYM_ID'})


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#  ORDINARY OR PREFERRED EQUITY SHARES + ADR/GDR - COMPANIES THAT ISSUED THEM
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# I follow Ferreira & Matos (2008) methodology to subset my universe of
# securities. According to them, I keep only ordinary or preferred equity
# shares including ADR/GDRs.

is_eq_and_ad = pl.col('ISSUE_TYPE').is_in(['EQ', 'AD'])

is_prefeq = (pl.col('ISSUE_TYPE') == 'PF') & (pl.col('FREF_SECURITY_TYPE') == 'PREFEQ')

# Our universe of entities
eq_sec = own_sec_uni.filter(is_eq_and_ad | is_prefeq)
# I document 49,710 unique companies 
our_entities = list(eq_sec.unique('FACTSET_ENTITY_ID_FSYM_ID')['FACTSET_ENTITY_ID_FSYM_ID'])

# For each entity/firm, isolate the primary listing and ISO country 
plisting = eq_sec.unique(['FACTSET_ENTITY_ID_FSYM_ID', 'FSYM_PRIMARY_EQUITY_ID'])
plisting_ = plisting.select(['FACTSET_ENTITY_ID_FSYM_ID', 
                             'FSYM_PRIMARY_EQUITY_ID',                            
                             'ISO_COUNTRY_ENTITY'])


# Add ISIN for primary listing 
# The ISIN for primary listing (-S) is missing for 1,180 companies
plisting_ = plisting_.join(sym_isin.rename({'FSYM_ID' : 'FSYM_PRIMARY_EQUITY_ID'}),
                           how='left',
                           on=['FSYM_PRIMARY_EQUITY_ID'])




# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#  KEEP ONLY ENTITIES THAT HAVE ISSUED ORDINARY OR PREFERRED EQUITY SHARES + ADR/GDR
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


# The filtering by entities that have issued ordinary or preferred equity shares
# including ADR/GDRs is the same as the filtering by ordinary or preffered equity
# shares including ADR/GDRs. Thus I use the cleaner filtering by entities or 
# companies. By applying the filter Half of the full holdings FactSet data 
# is dropped resulting in a dataframe with 46_623_132 rows.
fh_ = fh.filter(pl.col('FACTSET_ENTITY_ID_FSYM_ID').is_in(set(our_entities)))



# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#  AGGREGATE HOLDINGS OVER COMPANY-QUARTER-ENTITY/INSTITUION
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Total holdings of a company for an entity/institution - quarter pair
fh_company = (
    fh_
    .group_by(['FACTSET_ENTITY_ID_FSYM_ID', 'FACTSET_ENTITY_ID', 'date_q'])
    .agg(pl.col('MCAP_HELD').sum())
    )

# Total market cap of company
mcap_total = (
    fh_
    .unique(['FACTSET_ENTITY_ID_FSYM_ID', 'date_q'])
    .select(['FACTSET_ENTITY_ID_FSYM_ID', 'date_q', 'MCAP_TOTAL'])
    )

# Augment again with company market cap
fh_company = fh_company.join(mcap_total, how='left', on = ['FACTSET_ENTITY_ID_FSYM_ID', 'date_q'])

# Augment with primary equity listing identifier (-S) and ISO country
# for issuer entity/company
fh_company = fh_company.join(plisting_, how='left', on =['FACTSET_ENTITY_ID_FSYM_ID'])

# Free memory
del fh_, plisting, plisting_


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#      SOME HOUSEKEEPING
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# I have null total and holdings market cap values
fh_comp = fh_company.drop_nulls(['MCAP_HELD', 'MCAP_TOTAL'])

# I have zero total market cap values
fh_comp = fh_comp.filter(pl.col('MCAP_TOTAL')>0)



# ~~~~~~~
# SAVE 
# ~~~~~~~

fh_comp.write_parquet(os.path.join(factset_dir, 'factset_holdings_company_level.parquet'))


# COMPANY-LEVEL IO CHECK

f = fh_comp.with_columns(
    (pl.col('MCAP_HELD') / pl.col('MCAP_TOTAL')).alias('IO')
    )


io_comp = (
      f
      .group_by(['FACTSET_ENTITY_ID_FSYM_ID', 'date_q'])
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






