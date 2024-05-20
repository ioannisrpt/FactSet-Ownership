
# -*- coding: utf-8 -*-
"""
Concatenate datasets constructed from the 4 Schemes
"""


import os
import polars as pl

def any_duplicates(df, unique_cols):
    a = df.shape[0]
    b = df.unique(unique_cols).shape[0]
    print('Before unique: %d \n' % a )

    print('After unique: %d \n' %  b )

    print('Difference: %d' %(a-b))
    
main_cols = ['FSYM_ID', 'FACTSET_ENTITY_ID', 'date_q']

# ~~~~~~~~~~~~~
# DIRECTORIES 
# ~~~~~~~~~~~~~

# Current directory
cd = r'C:\Users\FMCC\Desktop\Ioannis'

# Parquet Factset tables
factset_dir =  r'C:\FactSet_Downloadfiles\zips\parquet'

# 13F filings
own_inst_13f_dir = os.path.join(factset_dir, 'own_inst_eq_v5_full')

# Stakes based sources including UKSR, RNS, 13D/G's, proxies, etc.
own_stakes_dir = os.path.join(factset_dir, 'own_stakes_eq_v5_full')

# Sum of Fund holdings
own_funds_dir = os.path.join(factset_dir, 'own_fund_eq_v5_full')


# ~~~~~~~~~~~~~~~~
#   IMPORT DATA
# ~~~~~~~~~~~~~~~~


scheme_1 = pl.read_parquet(os.path.join(cd, 'scheme_1_mcap_held.parquet'))
#any_duplicates(scheme_1, main_cols)

scheme_2 = pl.read_parquet(os.path.join(cd, 'scheme_2_mcap_held.parquet'))
#any_duplicates(scheme_2, main_cols)

scheme_3 = pl.read_parquet(os.path.join(cd, 'scheme_3_mcap_held.parquet'))
#any_duplicates(scheme_3, main_cols)

scheme_4 = pl.read_parquet(os.path.join(cd, 'scheme_4_mcap_held.parquet'))
#any_duplicates(scheme_4, main_cols)

# ~~~~~~~~~~~~
#   CONCAT
# ~~~~~~~~~~~~

fh = pl.concat([scheme_1,
                scheme_2,
                scheme_3,
                scheme_4])


# Sort
#fh = fh.sort(['FSYM_ID', 'FACTSET_ENTITY_ID', 'date_q'])

# Save
fh.write_parquet(os.path.join(cd, 'factset_mcap_holdings.parquet' ))



