# -*- coding: utf-8 -*-
"""
Factset methodology for institutional ownership calculations


Users can choose the order in which the sources are selected, 
if there is more than one source available for a 
given report date.

4. For UKSR securities - Scheme 4
-----------------------

This logic should be used for UKSR securities (own_sec_coverage.fds_uksr_flag=1).

• Use the UKSR or RNS position in the own_inst_stakes_detail table where the source code in
(‘W’,’Q’,’H’) if the as_of_date is within 18 months of the perspective date.
• If there is no stakes-base source, or if the report_date is older than 18 months, sum of funds
can be used if available


Output:
    scheme_4.parquet


"""


import os
import polars as pl
import pandas as pd

def any_duplicates(df, unique_cols):
    a = df.shape[0]
    b = df.unique(unique_cols).shape[0]
    print('Before unique: %d \n' % a )

    print('After unique: %d \n' %  b )

    print('Difference: %d' %(a-b))
    
main_cols = ['FSYM_ID', 'FACTSET_ENTITY_ID', 'date_q']



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



# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#        IMPORT DATA
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Import own_ent_institutions table
own_ent_inst = pl.read_parquet(os.path.join(factset_dir, 'own_ent_institutions.parquet'),
                               use_pyarrow=True)

# Import own_sec_coverage table
own_sec_cov = pl.read_parquet(os.path.join(factset_dir, 'own_sec_coverage_eq.parquet'),
                              use_pyarrow=True)

# Import own_ent_funds table 
own_ent_funds = pl.read_parquet(os.path.join(factset_dir, 'own_ent_funds.parquet'),
                                use_pyarrow=True)
# the table is used to match a Fund to the Institution that manages it
own_ent_funds = ( 
            own_ent_funds
            .select(['FACTSET_FUND_ID', 'FACTSET_INST_ENTITY_ID'])
            .rename({'FACTSET_INST_ENTITY_ID': 'FACTSET_ENTITY_ID'})
            )


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#    FORMAT OWN_INST_STAKES TABLE
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
own_inst_stakes = pl.read_parquet(os.path.join(own_inst_13f_dir, 'own_inst_stakes_detail_eq.parquet'),
                             use_pyarrow=True)
# Define quarter date 'date_q'
own_inst_stakes = apply_quarter_scheme(own_inst_stakes, 'AS_OF_DATE')

# Isolate columns for Scheme 4 (UKSR securities)
own_inst_stakes_uksr =  own_inst_stakes.select(['FSYM_ID',
                                            'FACTSET_ENTITY_ID',
                                            'AS_OF_DATE',
                                            'SOURCE_CODE',
                                            'POSITION',
                                            'date_q'])

# Isoalate columns for Scheme 1, 2, 3
own_inst_stakes =  own_inst_stakes.select(['FSYM_ID',
                                            'FACTSET_ENTITY_ID',
                                            'AS_OF_DATE',
                                            'POSITION',
                                            'date_q'])

# ///////////////////////////////////////////////////////

#         For UKSR securities  - SCHEME 4

# ///////////////////////////////////////////////////////

print('UKSR securities - SCHEME 4 \n')


# UKSR securities
security_uksr = own_sec_cov.filter(pl.col('FDS_UKSR_FLAG') == 1) 
security_uksr_set = set(security_uksr['FSYM_ID'])

# Source code for Stakes
source_code_uksr = set(['W', 'Q', 'H'])

# -----------------------------
#     MASTER DATAFRAME - SETTING UP 
# -----------------------------

# Define a master dataframe for scheme 4 (similar to scheme 3)
# The master dataframe will have as rows all combinations of
# institutions + UKSR securities + quarter dates

# Quarter dates (including 202312 and excluding 202403)
date_range = pd.date_range(start = pd.to_datetime('198809', format='%Y%m'), 
                         end   = pd.to_datetime('202403', format='%Y%m'), 
                         freq  = 'Q')

date_range_int = [int(x.strftime('%Y%m')) for x in date_range]
quarters_pl = pl.DataFrame({'date_q' :date_range_int }).cast(pl.Int32)


# First I need to find all the valid holder + security combinations
# and then then expand on quarter dates 

# ----------------
#  STAKES table
# --------------

# Finding the valid holder-securities pairs from Stakes Table
# Filter for UKSR security + any holder
stakes_pairs = (
    own_inst_stakes_uksr.filter(
    pl.col('FSYM_ID').is_in(security_uksr_set) &
    pl.col('SOURCE_CODE').is_in(source_code_uksr)
    )
    .unique(['FSYM_ID', 'FACTSET_ENTITY_ID'])
    .select(['FSYM_ID', 'FACTSET_ENTITY_ID'])
    )


# -----------------
#   SUM OF FUNDS table
# -----------------------


# Finding the valid holder-securities pairs from Funds Tables
funds_pairs = pl.DataFrame()

# Iterate through Sum of Funds datasets
for dataset in os.listdir(own_funds_dir):
    
    print('%s is processed. \n' % dataset)
    
    # Import sum of funds dataset
    own_fund = pl.read_parquet(os.path.join(own_funds_dir, dataset),
                               use_pyarrow=True)
    
    
    # Merge Fund with Institution that manages the Fund
    own_fund = own_fund.join(own_ent_funds, 
                             how='inner',
                             on='FACTSET_FUND_ID')

    # Filter 
    funds_pairs_ =  (
        own_fund.filter(
        pl.col('FSYM_ID').is_in(security_uksr_set) 
        )
        .unique(['FSYM_ID', 'FACTSET_ENTITY_ID'])
        .select(['FSYM_ID', 'FACTSET_ENTITY_ID'])
        )
        
    # Concat
    funds_pairs = pl.concat([funds_pairs, funds_pairs_])
    
    
# Concat and keep unique
valid_pairs = pl.concat([stakes_pairs, funds_pairs]).unique()


scheme_4 = valid_pairs.join(quarters_pl, how='cross')

# Free memory
del stakes_pairs, funds_pairs



# ----------------------------
#   GET POSITIIONS FOR STAKES 
# -----------------------------

# Keep only the most recent 'AS_OF_DATE' observation within quarter
own_inst_stakes_ = ( 
    own_inst_stakes_uksr
    .group_by(['FSYM_ID', 'FACTSET_ENTITY_ID', 'date_q'])
    .agg([pl.all().sort_by('AS_OF_DATE').last()])
    )

stakes_positions = (
    own_inst_stakes_.filter(
    pl.col('FSYM_ID').is_in(security_uksr_set) &
    pl.col('SOURCE_CODE').is_in(source_code_uksr)
    )
    .select(['FSYM_ID','FACTSET_ENTITY_ID', 'date_q', 'POSITION' ])
    .rename({'POSITION' : 'ADJ_SHARES_HELD_STAKES'})
    )


# MERGE WITH MASTER DATAFRAME
scheme_4 = scheme_4.join(stakes_positions, how='left',
                         on=['FSYM_ID', 'FACTSET_ENTITY_ID', 'date_q'])


# ---------------------------------
#   GET POSITIONS FOR FUNDS
# ---------------------------------

# Define DataFrame to store
funds_positions = pl.DataFrame()


# Iterate through Sum of Funds datasets
for dataset in os.listdir(own_funds_dir):
    
    print('%s is processed. \n' % dataset)
    
    # Import sum of funds dataset
    own_fund = pl.read_parquet(os.path.join(own_funds_dir, dataset),
                               use_pyarrow=True)
    
    
    # Merge Fund with Institution that manages the Fund
    own_fund = own_fund.join(own_ent_funds, 
                             how='inner',
                             on='FACTSET_FUND_ID')
    

    # Filter 
    own_fund_ =  own_fund.filter(
        pl.col('FSYM_ID').is_in(security_uksr_set) 
        )
    
    # Define quarter
    own_fund_ = apply_quarter_scheme(own_fund_, 'REPORT_DATE')
    
    # Keep only the most recent 'REPORT_DATE' within a quarter for
    # a security-fund pair
    own_fund_ = ( 
        own_fund_
        .sort(['FSYM_ID', 'FACTSET_FUND_ID', 'date_q', 'REPORT_DATE'])
        .group_by(['FSYM_ID', 'FACTSET_FUND_ID', 'date_q'])
        .agg([pl.all().sort_by('REPORT_DATE').last()])
        )
    
    own_fund_ = own_fund_.select(['FSYM_ID',
                                  'FACTSET_ENTITY_ID',
                                  'REPORT_DATE',
                                  'ADJ_HOLDING',
                                  'REPORTED_HOLDING',
                                  'date_q'])
    
    
    own_fund_inst = ( 
        own_fund_
        .group_by(['FSYM_ID', 'FACTSET_ENTITY_ID', 'date_q'])
        .agg(pl.col('ADJ_HOLDING').sum())
        )
    
    # Concat 
    funds_positions = pl.concat([funds_positions, own_fund_inst])
 
    
# Sum positions again because a security in a quarter that belongs to a different
# fund  under the same institution might appear in a different own_fund_eq table 
funds_positions = ( 
    funds_positions
    .group_by(['FSYM_ID', 'FACTSET_ENTITY_ID', 'date_q'])
    .agg(pl.col('ADJ_HOLDING').sum())
    .rename({'ADJ_HOLDING' : 'ADJ_SHARES_HELD_FUNDS'})
    )    
    



# MERGE WITH MASTER DATAFRAME
scheme_4 = scheme_4.join(funds_positions, how='left',
                         on=['FSYM_ID', 'FACTSET_ENTITY_ID', 'date_q'])


# ----------------------
#   WINDOW OF 18 MONTHS
# ------------------------

# Compare the perspective date to the as_of_date and forward fill null positions
# using an 18 month or 6 quarter window.
scheme_4 = scheme_4.with_columns(
            pl.col('ADJ_SHARES_HELD_STAKES')
            .forward_fill(limit=6)
            .over(['FSYM_ID', 'FACTSET_ENTITY_ID'])
            .alias('ADJ_SHARES_HELD_STAKES_FILLED')
            )

# Replace stake position with a fund position 
# only if a stake position is null and a fund position exists
scheme_4_final = ( 
    scheme_4.with_columns(
    pl.when(pl.col('ADJ_SHARES_HELD_STAKES_FILLED').is_null() & 
            (~pl.col('ADJ_SHARES_HELD_FUNDS').is_null()) )
    .then(pl.col('ADJ_SHARES_HELD_FUNDS'))
    .otherwise(pl.col('ADJ_SHARES_HELD_STAKES_FILLED'))
    .alias('ADJ_SHARES_HELD')
    )
    .select(['FSYM_ID', 'FACTSET_ENTITY_ID', 'date_q', 'ADJ_SHARES_HELD' ])
    .drop_nulls()
    )


# Define the scheme
scheme_4_final = scheme_4_final.with_columns(
   pl.lit(4).alias('SCHEME')               
    ) 

# Free memory
del scheme_4, funds_positions, stakes_positions, own_fund, own_fund_, own_fund_inst

# ~~~~~~~~~~~~~~
#   SAVE
# ~~~~~~~~~~~


scheme_4_final.write_parquet(os.path.join(cd, 'scheme_4.parquet'))

