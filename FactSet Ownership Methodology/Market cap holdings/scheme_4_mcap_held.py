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

Position is in terms of market capitalization holdings.

Output:
    scheme_4_mcap_held.parquet


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

# Prices
own_sec_prices = pl.read_parquet(os.path.join(factset_dir, 'own_sec_prices_eq.parquet'),
                                 use_pyarrow=True)

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#    FORMAT OWN_INST_STAKES TABLE
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
own_inst_stakes = pl.read_parquet(os.path.join(own_inst_13f_dir, 'own_inst_stakes_detail_eq.parquet'),
                             use_pyarrow=True)

# Define quarter date 'date_q'
own_inst_stakes = apply_quarter_scheme(own_inst_stakes, 'AS_OF_DATE')

# Keep only positive positions
own_inst_stakes = own_inst_stakes.filter(pl.col('POSITION')>0)

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



# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#    FORMAT OWN_SEC_PRICES TABLE
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Define quarter date 'date_q'
own_sec_prices = apply_quarter_scheme(own_sec_prices, 'PRICE_DATE')


# Keep only the most recent 'price' observation within a quarter
# for each security (data already sorted)
own_sec_prices_q = ( 
    own_sec_prices
    .group_by(['FSYM_ID', 'date_q'])
    .agg(pl.all().sort_by('PRICE_DATE').last())
    )


# Prices only
prices_q = ( own_sec_prices_q
             .select(['FSYM_ID', 
                      'date_q', 
                      'ADJ_PRICE',
                      'UNADJ_PRICE'])
             )

# Keep only positive unadjusted prices
prices_q = prices_q.filter(pl.col('UNADJ_PRICE')>0)

# Adjusted price of 0 is treated as null
prices_q = (
    prices_q.with_columns(
        pl.when(pl.col('ADJ_PRICE') == 0)
        .then(None)
        .otherwise(pl.col('ADJ_PRICE'))
        .alias('ADJ_PRICE')
        )
    )


# ///////////////////////////////////////////////////////

#         For UKSR securities  - SCHEME 4

# ///////////////////////////////////////////////////////

print('UKSR securities - SCHEME 4 (MCAP HELD) \n')


# UKSR securities
security_uksr = own_sec_cov.filter(pl.col('FDS_UKSR_FLAG') == 1) 
security_uksr_set = set(security_uksr['FSYM_ID'])

# Source code for Stakes
source_code_uksr = set(['W', 'Q', 'H'])

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#     MASTER DATAFRAME - SETTING UP THE SECURITY+HOLDER+QUARTER PAIR
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

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
    
    # Keep positive positions
    own_fund = own_fund.filter(pl.col('REPORTED_MV')>0)
    
    
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
    
    
# Concat and keep unique pairs
funds_pairs = funds_pairs.unique()

# All unique pairs
valid_pairs = pl.concat([stakes_pairs, funds_pairs]).unique()


scheme_4 = valid_pairs.join(quarters_pl, how='cross')

# Free memory
del stakes_pairs, funds_pairs



# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#   POSITIIONS FROM STAKES TABLE
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


# Filter for UKSR securities
own_inst_stakes_ = own_inst_stakes_uksr.filter(
    pl.col('FSYM_ID').is_in(security_uksr_set) &
    pl.col('SOURCE_CODE').is_in(source_code_uksr)
    )


    

# Keep only the most recent 'AS_OF_DATE' observation within quarter
own_inst_stakes_ = ( 
    own_inst_stakes_
    .group_by(['FSYM_ID', 'FACTSET_ENTITY_ID', 'date_q'])
    .agg(pl.all().sort_by('AS_OF_DATE').last())
    )

# Re-order and keep cols
own_inst_stakes_ = ( 
                    own_inst_stakes_
                    .select(['FSYM_ID',
                            'FACTSET_ENTITY_ID',
                            'AS_OF_DATE',
                            'POSITION',
                            'date_q'])
                    .drop_nulls()
                    )



# --------------------------
#  AUGMENT WITH MARKET CAP
# -------------------------


# Join with prices
own_inst_stakes_ = own_inst_stakes_.join(prices_q, 
                                         how='left', 
                                         on=['FSYM_ID', 'date_q'])
              
# Market cap holdings
own_inst_stakes_ = own_inst_stakes_.with_columns(
    (pl.col('POSITION') * pl.col('ADJ_PRICE')).alias('MCAP_HELD_STAKES')
    )

# Keep necessary columns and drop rows where market cap holdings are missing
stakes_positions = (
    own_inst_stakes_
    .select(['FSYM_ID', 
             'FACTSET_ENTITY_ID',
             'date_q',
             'AS_OF_DATE',
             'MCAP_HELD_STAKES'])
    .drop_nulls(['MCAP_HELD_STAKES'])
    )

# -----------------------------------------------------
#           MERGE WITH MASTER DATAFRAME
# -----------------------------------------------------


scheme_4 = scheme_4.join(stakes_positions, how='left',
                         on=['FSYM_ID', 'FACTSET_ENTITY_ID', 'date_q'])


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#    POSITIONS FROM FUNDS TABLE
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Define DataFrame to store
funds_positions = pl.DataFrame()


# Iterate through Sum of Funds datasets
for dataset in os.listdir(own_funds_dir):
    
    print('%s is processed. \n' % dataset)
    
    # Import sum of funds dataset
    own_fund = pl.read_parquet(os.path.join(own_funds_dir, dataset),
                               use_pyarrow=True)
    
    # Keep positive positions
    own_fund = own_fund.filter(pl.col('REPORTED_MV')>0)
    
    
    # Merge Fund with Institution that manages the Fund
    own_fund = own_fund.join(own_ent_funds, 
                             how='inner',
                             on='FACTSET_FUND_ID')
    

    # Filter for UKSR securites
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
        .agg(pl.all().sort_by('REPORT_DATE').last())
        )
    
    # Keep 'adjusted market value' if not missing, otherwise
    # keep 'reported market value' as 'market value'.
    own_fund_ = own_fund_.with_columns(
        pl.when(pl.col('ADJ_MV').is_not_null() & pl.col('ADJ_MV')>0)
        .then(pl.col('ADJ_MV'))
        .otherwise(pl.col('REPORTED_MV'))
        .alias('MCAP_HELD_FUNDS')
        )
    
    own_fund_ = own_fund_.select(['FSYM_ID',
                                  'FACTSET_ENTITY_ID',
                                  'REPORT_DATE',
                                  'date_q',
                                  'MCAP_HELD_FUNDS'])
    
    
    # Sum the position of an institution for each security within a quarter
    own_fund_inst = ( 
        own_fund_
        .group_by(['FSYM_ID', 'FACTSET_ENTITY_ID', 'date_q'])
        .agg(pl.col('MCAP_HELD_FUNDS').sum())
        )
    
    # Concat 
    funds_positions = pl.concat([funds_positions, own_fund_inst])
 
    
# Sum positions again because a security in a quarter that belongs to a different
# fund  under the same institution might appear in a different own_fund_eq table 
funds_positions = ( 
    funds_positions
    .group_by(['FSYM_ID', 'FACTSET_ENTITY_ID', 'date_q'])
    .agg(pl.col('MCAP_HELD_FUNDS').sum())
    )    
    


# -----------------------------------------------------
#           MERGE WITH MASTER DATAFRAME
# -----------------------------------------------------

scheme_4 = scheme_4.join(funds_positions, how='left',
                         on=['FSYM_ID', 'FACTSET_ENTITY_ID', 'date_q'])


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#           WINDOW OF 18 MONTHS FOR UKSR SECURITIES 
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Compare the perspective date to the as_of_date and forward fill null positions
# using an 18 month or 6 quarter window.
scheme_4 = scheme_4.with_columns(
            pl.col('MCAP_HELD_STAKES')
            .forward_fill(limit=6)
            .over(['FSYM_ID', 'FACTSET_ENTITY_ID'])
            .alias('MCAP_HELD_STAKES_FILLED')
            )


# Use a filled stakes position if it exists.
# Otherwise use a funds position.
scheme_4_final = ( 
    scheme_4.with_columns(
    pl.when(pl.col('MCAP_HELD_STAKES_FILLED').is_not_null())
    .then(pl.col('MCAP_HELD_STAKES_FILLED'))
    .otherwise(pl.col('MCAP_HELD_FUNDS'))
    .alias('MCAP_HELD')
    )
    )


# Free memory
del scheme_4, funds_positions, stakes_positions, own_fund, own_fund_, own_fund_inst

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~
#    SOME HOUSEKEEPING
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Select and drop nulls
scheme_4_final = (
    scheme_4_final
    .select(['FSYM_ID', 'FACTSET_ENTITY_ID', 'date_q', 'MCAP_HELD' ])
    .drop_nulls()
    )


# Define the scheme
scheme_4_final = scheme_4_final.with_columns(
   pl.lit(4).alias('SCHEME')               
    ) 


# ~~~~~~~~~~~~~~
#   SAVE
# ~~~~~~~~~~~


scheme_4_final.write_parquet(os.path.join(cd, 'scheme_4_mcap_held.parquet'))

