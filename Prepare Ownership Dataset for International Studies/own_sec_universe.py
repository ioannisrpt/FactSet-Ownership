# -*- coding: utf-8 -*-
"""
Identify the universe of securities found in Ownership bundle
and create a master dataset that contains all relevant information
about these securities. 

Relevant information is
    - the country they are domiciled 
    - the security type (fref_security_type)
    - the universe (universe_type) 
    - entity type (entity_type)
    - primary equity (fsym_primary_equity_id)


Inputs:
    own_sec_coverage.parquet
    own_sec_entity.parquet
    sym_xc_isin.parquet
    sym_coverage.parquet
    sym_entity.parquet
    
    
Output:
    own_sec_universe.parquet
"""



import os
import polars as pl



# DIRECTORIES 

r"""
# Current directory
cd = r'C:\Users\FMCC\Desktop\Ioannis'

# Parquet Factset tables
factset_dir =  r'C:\FactSet_Downloadfiles\zips\parquet'
"""

factset_dir = r'C:\Users\ropot\Desktop\Financial Data for Research\FactSet'

# ~~~~~~~~~~~~~~~~~~
#   IMPORT DATA
# ~~~~~~~~~~~~~~~~~~

# Core of the dataset is the universe of ownership securities (Ownership coverage)
own_sec_cov = pl.read_parquet(os.path.join(factset_dir, 'own_sec_coverage_eq.parquet'),
                              use_pyarrow=True, columns=['FSYM_ID', 
                                                         'ISO_COUNTRY',
                                                         'ISSUE_TYPE',
                                                         'UNIVERSE_TYPE'])
own_sec_cov = own_sec_cov.rename({'UNIVERSE_TYPE' : 'UNIVERSE_TYPE_OWN',
                                  'ISO_COUNTRY' : 'ISO_COUNTRY_SECURITY'})


# Mapping of securities to entities (symbology convention)
own_sec_ent = pl.read_parquet(os.path.join(factset_dir, 'own_sec_entity_eq.parquet'),
                              use_pyarrow=True)


# Isins
sym_isin = pl.read_parquet(os.path.join(factset_dir, 'sym_xc_isin.parquet'))


# Symbology coverage
sym_cov = pl.read_parquet(os.path.join(factset_dir, 'sym_coverage.parquet'),
                              use_pyarrow=True, columns=['FSYM_ID', 
                                                         'FREF_SECURITY_TYPE',
                                                         'UNIVERSE_TYPE',
                                                         'FSYM_PRIMARY_EQUITY_ID'])
sym_cov = sym_cov.rename({'UNIVERSE_TYPE' : 'UNIVERSE_TYPE_SYM'})


# Information at the entity/company level
sym_ent =  pl.read_parquet(os.path.join(factset_dir, 'sym_entity.parquet'),
                           use_pyarrow=True, columns=['FACTSET_ENTITY_ID', 
                                                      'ENTITY_TYPE',
                                                      'ENTITY_PROPER_NAME',
                                                      'ISO_COUNTRY'])
sym_ent = sym_ent.rename({'ISO_COUNTRY' : 'ISO_COUNTRY_ENTITY'})


# ~~~~~~~~~~~~~~~~~~
#    MERGE
# ~~~~~~~~~~~~~~~~~~

# + ISIN
own_securities = own_sec_cov.join(sym_isin, how='left', on=['FSYM_ID'])

# + FREF_SECURITY_TYPE + UNIVERSE_TYPE + PRIMARY EQUITY ID
own_securities = own_securities.join(sym_cov, how='left', on=['FSYM_ID'])

# + FACTSET_ENTITY_ID 
own_securities = own_securities.join(own_sec_ent, how='left', on=['FSYM_ID'])

# + ENTITY_TYPE + ENTITY NAME
own_securities = own_securities.join(sym_ent, how='left', on=['FACTSET_ENTITY_ID'])


# ~~~~~~
#  SAVE
# ~~~~~~

own_securities.write_parquet(os.path.join(factset_dir, 'own_sec_universe.parquet'))


