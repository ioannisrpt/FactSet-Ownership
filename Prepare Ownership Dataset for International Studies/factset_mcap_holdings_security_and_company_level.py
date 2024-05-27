# -*- coding: utf-8 -*-
"""
Aggregate institutional ownership market cap holdings at the company level.

A firm might have issued multiple securities that are in turn owned by
different or the same institutions. Thus market holdings have to be aggregated
at the company rather than the security level.

Augment the dataset with the total market cap of the company for
IO calculations.


Notes:
    1. There are cases when the holdings of an institution exceed the market 
    cap value of a security. I exclude such security-holder-quarter pairs 
    from the security level dataset.
    
    2. There are cases when the holdings of an institution exceed the market 
    cap value of a company. I exclude such company-holder-quarter pairs 
    from the company level dataset.

Input:
    factset_mcap_holdings.parquet
    mcap_total_company_level.parquet
    own_sec_universe.parquet
    sym_xc_isin.parquet
    
Output:
    factset_mcap_holdings_security_level.parquet
    factset_mcap_holdings_company_level.parquet
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

# Ownership market cap holdings at the security level
"""
fh = pl.read_parquet(os.path.join(factset_dir, 'factset_mcap_holdings.parquet'),
                              use_pyarrow=True)
"""

fh = pl.read_parquet(os.path.join(cd, 'factset_mcap_holdings.parquet'),
                              use_pyarrow=True)

# Total market cap at the company level
"""
mcap_total = pl.read_parquet(os.path.join(factset_dir, 'mcap_total_company_level.parquet'),
                              use_pyarrow=True) 
"""

mcap_total = pl.read_parquet(os.path.join(cd, 'mcap_total_company_level.parquet'),
                              use_pyarrow=True) 


# Full universe of securities covered in ownership bundle along with additional info
"""
own_sec_uni = pl.read_parquet(os.path.join(factset_dir, 'own_sec_universe.parquet'),
                                 use_pyarrow=True)
"""

own_sec_uni = pl.read_parquet(os.path.join(cd, 'own_sec_universe.parquet'),
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

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

#  MAPPING SECURITIES TO ENTITIES
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Augment with company identifier (-E)
security_company_map = own_securities.select(['FSYM_ID',
                                              'FACTSET_ENTITY_ID_FROM_FSYM_ID'])
fh_ = fh_.join(security_company_map,
               how='left',
               on=['FSYM_ID'])

# Mapping is not perfect and I lose market cap holdings. I have 289 null 
# values for company id.
# How do I fix this?
# Use historical mappings of securities to companies?

# Keep only securities that have been mapped to an entity.
fh_ = fh_.filter(pl.col('FACTSET_ENTITY_ID_FROM_FSYM_ID').is_not_null())


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#   HOLDINGS AT THE SECURITY LEVEL 
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Augment again with company market capitalization
# I have missing values of market cap.
fh_sec = fh_.join(mcap_total, 
                how='left', 
                on = ['FACTSET_ENTITY_ID_FROM_FSYM_ID', 'date_q'])


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



# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#   HOLDINGS AT THE COMPANY LEVEL 
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


# Total holdings of a company for an entity/institution - quarter pair
fh_comp = (
    fh_sec
    .group_by(['FACTSET_ENTITY_ID_FROM_FSYM_ID', 'FACTSET_ENTITY_ID', 'date_q'])
    .agg(pl.col('MCAP_HELD').sum())
    )

fh_comp = fh_comp.drop_nulls()

# Augment again with company market capitalization
# I have missing values of market cap.
fh_comp = fh_comp.join(mcap_total, 
                       how='left', 
                       on = ['FACTSET_ENTITY_ID_FROM_FSYM_ID', 'date_q'])



# Augment with primary equity listing identifier (-S) and ISO country
# for issuer entity/company
#fh_company = fh_company.join(plisting_, how='left', on =['FACTSET_ENTITY_ID_FSYM_ID'])

# Free memory
del fh_, plisting, plisting_


# -----------------
# SOME HOUSEKEEPING
# -----------------


# Purge any null values
fh_comp = fh_comp.drop_nulls()

# Keep only positive market cap if any
fh_comp = fh_comp.filter((pl.col('MCAP_TOTAL')>0) &
                         (pl.col('MCAP_HELD')>0)  )



# If holdings exceed market cap value at the company level, then
# the holdings are deemed invalid and dropped.
fh_comp = fh_comp.with_columns(
    (pl.col('MCAP_HELD') / pl.col('MCAP_TOTAL')).alias('IO')
    )

fh_comp = fh_comp.filter(pl.col('IO') <= 1)



# Final form of dataset
fh_comp = (
    fh_comp
    .select(['FACTSET_ENTITY_ID_FROM_FSYM_ID',
             'FACTSET_ENTITY_ID',
             'date_q',
             'MCAP_HELD',
             'MCAP_TOTAL'])
    .sort(by=['FACTSET_ENTITY_ID_FROM_FSYM_ID',
              'FACTSET_ENTITY_ID',
              'date_q'])
    )



# ~~~~~~~
# SAVE 
# ~~~~~~~

#fh_comp.write_parquet(os.path.join(factset_dir, 'factset_mcap_holdings_company_level.parquet'))

fh_comp.write_parquet(os.path.join(cd, 'factset_mcap_holdings_company_level.parquet'))












