# -*- coding: utf-8 -*-
"""
Replication of Ferreira & Matos (2008) methodology

PART 4 - FETCH PRINCIPAL SECURITY FOR EACH COMPANY

Market cap is in millions of USD.

Input:
    holdingsall_company_level.parquet

    
Output:
    entity_identifiers.parquet
    holdingsall_company_level_v2.parquet
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


# Onwership holdings at the company level
holdingsall = pl.read_parquet(os.path.join(cd, 'holdingsall_company_level.parquet'))

# own_sec_entity : map fsym_id to factset_entity_id for Ownership securities
own_sec_entity = pl.read_parquet(os.path.join(factset_dir, 'own_sec_entity_eq.parquet'))

# sym_coverage : coverage of securities in Symbology bundle
sym_coverage = pl.read_parquet(os.path.join(factset_dir, 'sym_coverage.parquet'),
                               columns=['FSYM_ID',
                                        'FSYM_PRIMARY_EQUITY_ID',
                                        'FSYM_PRIMARY_LISTING_ID',
                                        'FREF_SECURITY_TYPE',
                                        'ACTIVE_FLAG'])

# ISIN tables
sym_isin = pl.read_parquet(os.path.join(factset_dir, 'sym_isin.parquet'))

sym_xc_isin = pl.read_parquet(os.path.join(factset_dir, 'sym_xc_isin.parquet'))
sym_xc_isin = sym_xc_isin.rename({'ISIN' : 'XC_ISIN'})

# CUSIP table
sym_cusip = pl.read_parquet(os.path.join(factset_dir, 'sym_cusip.parquet'))

# TICKER table
sym_ticker_region = pl.read_parquet(os.path.join(factset_dir, 'sym_ticker_region.parquet'))


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#    FETCH PRINCIPAL SECURITY
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Companies/entities found in holdings
own_companies = list(holdingsall.select(['COMPANY_ID']).unique()['COMPANY_ID'])

# Only companies found in holdings
#own_sec_entity_ = own_sec_entity.filter(pl.col('FACTSET_ENTITY_ID').is_in(companies))

# principal_security TABLE 
principal_security = sym_coverage.join(own_sec_entity,
                                       how='left',
                                       on=['FSYM_ID'])
principal_security = ( 
    principal_security
    .drop_nulls(['FACTSET_ENTITY_ID'])
    .filter(pl.col('FACTSET_ENTITY_ID').is_in(own_companies))
    .filter(pl.col('FSYM_ID') == pl.col('FSYM_PRIMARY_EQUITY_ID'))
    .sort(by=['FACTSET_ENTITY_ID'])
    )

principal_security = principal_security.unique(['FACTSET_ENTITY_ID'])

# Companies found in principal_security
companies_in_principal_security = ( 
    list(principal_security
         .select(['FACTSET_ENTITY_ID'])
         .unique()['FACTSET_ENTITY_ID'])
    )

# remaining_securities TABLE
remaining_securities = sym_coverage.join(own_sec_entity,
                                         how='left',
                                         on=['FSYM_ID'])

remaining_securities = ( 
    remaining_securities
    .drop_nulls(['FACTSET_ENTITY_ID'])
    .filter(pl.col('FACTSET_ENTITY_ID').is_in(own_companies))
    .filter(~pl.col('FACTSET_ENTITY_ID').is_in(companies_in_principal_security))
    .filter(pl.col('FREF_SECURITY_TYPE').is_in(['SHARE', 'PREFEQ']))
    .sort(by=['FACTSET_ENTITY_ID', 'ACTIVE_FLAG', 'FREF_SECURITY_TYPE'])
    )

remaining_securities = remaining_securities.unique(['FACTSET_ENTITY_ID'], keep='first')



# security_entity1 TABLE: security-company pairs 
security_entity1 = pl.concat([principal_security.select(['FSYM_ID', 'FACTSET_ENTITY_ID']),
                              remaining_securities.select(['FSYM_ID', 'FACTSET_ENTITY_ID'])])


# security_entity TABLE : augment security-company pairs with primary listing
security_entity = security_entity1.join(sym_coverage.select(['FSYM_ID', 'FSYM_PRIMARY_LISTING_ID']),
                                        how='left',
                                        on=['FSYM_ID'])

# entity_identifiers TABLE: complete security information for company
entity_identifiers = ( 
    security_entity
    .join(sym_isin, how='left',on=['FSYM_ID'])
    .join(sym_xc_isin, how='left',on=['FSYM_ID'])
    .join(sym_cusip, how='left', on =['FSYM_ID'])
    .join(sym_ticker_region, how='left', left_on=['FSYM_PRIMARY_LISTING_ID'], right_on=['FSYM_ID'])
    )

entity_identifiers = ( 
    entity_identifiers
    .with_columns(
        pl.when(pl.col('ISIN').is_null())
        .then(pl.col('XC_ISIN'))
        .otherwise(pl.col('ISIN'))
        .alias('ISIN')
        )
    .drop(['XC_ISIN'])
    )

entity_identifiers = entity_identifiers.rename({'FACTSET_ENTITY_ID' : 'COMPANY_ID'})

# Free memory
del sym_cusip, sym_coverage, sym_isin, sym_xc_isin, sym_ticker_region



# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#    AUGMENT HOLDINGS WITH PRIMARY SECURITY INFORMATION
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


holdingsall = holdingsall.join(entity_identifiers,
                               how='left',
                               on=['COMPANY_ID'])


# SAVE
entity_identifiers.write_parquet(os.path.join(cd, 'entity_identifiers.parquet'))

holdingsall.write_parquet(os.path.join(cd, 'holdingsall_company_level_v2.parquet'))


"""
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~
#   SANITY CHECKS - WHAT IF EXCLUDE NO ISIN POSITIONS?
# ~~~~~~~~~~~~~~~~~~~~~~


# No ISIN positions
holdingsall_ = ( 
    holdingsall
    .drop_nulls(['ISIN'])
    .select(['FACTSET_ENTITY_ID',
             'FSYM_ID',
             'COMPANY_ID',
             'date_q',
             'MKTCAP_HELD'])
    )



# Sum of all institutional holdings 
mcap_held = (
    holdingsall_
    .group_by('date_q')
    .agg(pl.col('MKTCAP_HELD').sum())
    .sort(by='date_q')
    )

# Sum of market cap of securitites being owned
hmktcap = pl.read_parquet(os.path.join(cd, 'hmktcap.parquet'))
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
"""
