# -*- coding: utf-8 -*-
"""
Factset methodology for institutional ownership calculations


Users can choose the order in which the sources are selected, 
if there is more than one source available for a 
given report date.


3. For non-13F Holder and/or non-13F Securities - Scheme 3
-------------------------------------------------

This logic should be used if the holder is not a 13F filer (own_ent_institutions.fds_13f_flag=0) and/or
the security is not a 13F reportable security (own_sec_coverage.fds_13f_flag=0 and
own_sec_coverage.fds_13f_ca_flag=0).

• Use any stakes-based position in the own_inst_stakes_detail table using the window below to
compare the perspective date to the as_of_date:
o North-American traded securities (US & Canada): 18 months
o Global securities: 21 months
• If there is no stakes-base source, or if the report_date is outside of the window, sum of funds
can be used if available.


Position is in terms of adjusted shares.

Adjusted shares are used because adjusted positions can be used
in quarter-to-quarter analysis for the calculation of other variables in
an apples-to-apples setting.

Output:
    scheme_3_adj_shares_held.parquet


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

# Keep only positive positions
own_inst_stakes = own_inst_stakes.filter(pl.col('POSITION')>0)

# Isoalate columns for Scheme 1, 2, 3
own_inst_stakes =  own_inst_stakes.select(['FSYM_ID',
                                            'FACTSET_ENTITY_ID',
                                            'AS_OF_DATE',
                                            'POSITION',
                                            'date_q'])




# ///////////////////////////////////////////////////////

#    For non 13F Holder and non 13F Securities  - SCHEME 3

# ///////////////////////////////////////////////////////

print('13F Holder + non 13F Securities - SCHEME 3 (ADJ SHARES HELD) \n')



# non 13F holder
non_holder_13f = own_ent_inst.filter(pl.col('FDS_13F_FLAG') == 0) 
non_holder_13f_set = set(non_holder_13f['FACTSET_ENTITY_ID'])



# non 13F Canadian or US security or UKSR security 
own_sec_cov = own_sec_cov.with_columns(
    (pl.col('FDS_13F_FLAG') + pl.col('FDS_13F_CA_FLAG') + pl.col('FDS_UKSR_FLAG'))
    .alias('IS_13F_OR_UK')
    )

security_non_13f = own_sec_cov.filter(pl.col('IS_13F_OR_UK') == 0) 
security_non_13f_set = set(security_non_13f['FSYM_ID'])



# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#     MASTER DATAFRAME - SETTING UP THE SECURITY+HOLDER+QUARTER PAIR
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Define a master dataframe for scheme 3
# The master dataframe will have as rows all combinations of
# non 13F holders + non 13F US and Canadian or UK securities + quarter dates

# Quarter dates (including 202312 but excluding 202403)
date_range = pd.date_range(start = pd.to_datetime('198809', format='%Y%m'), 
                         end   = pd.to_datetime('202406', format='%Y%m'), 
                         freq  = 'Q')

date_range_int = [int(x.strftime('%Y%m')) for x in date_range]
quarters_pl = pl.DataFrame({'date_q' :date_range_int }).cast(pl.Int32)


# First I need to find all the valid holder + security combinations
# and then then expand on quarter dates 

# -------------
#  Stakes table
# --------------


# Finding the valid holder-securities pairs from Stakes Table
# Filter for non-13F or UK security and non 13f holder
stakes_pairs = (
    own_inst_stakes.filter(
    pl.col('FSYM_ID').is_in(security_non_13f_set) &
    pl.col('FACTSET_ENTITY_ID').is_in(non_holder_13f_set)
    )
    .unique(['FSYM_ID', 'FACTSET_ENTITY_ID'])
    .select(['FSYM_ID', 'FACTSET_ENTITY_ID'])
    )


# ---------------------
#   Funds table
# ----------------------

# Finding the valid holder-securities pairs from Funds Tables
funds_pairs = pl.DataFrame()

# Iterate through Sum of Funds datasets
for dataset in os.listdir(own_funds_dir):
    
    print('%s is processed. \n' % dataset)
    
    # Import sum of funds dataset
    own_fund = pl.read_parquet(os.path.join(own_funds_dir, dataset),
                               use_pyarrow=True)
    
    # Keep positive positions
    own_fund = own_fund.filter(pl.col('REPORTED_HOLDING')>0)
    
    
    # Merge Fund with Institution that manages the Fund
    own_fund = own_fund.join(own_ent_funds, 
                             how='inner',
                             on='FACTSET_FUND_ID')

    # Filter 
    funds_pairs_ =  (
        own_fund.filter(
        pl.col('FSYM_ID').is_in(security_non_13f_set) &
        pl.col('FACTSET_ENTITY_ID').is_in(non_holder_13f_set)
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

# Initialize scheme 3 dataframe
scheme_3 = valid_pairs.join(quarters_pl, how='cross')


# Free memory
del stakes_pairs, funds_pairs

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#   POSITIIONS FROM STAKES TABLE
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


# Filter for 13f Canadian security +  13f holder 
stakes_positions =  own_inst_stakes.filter(
    pl.col('FSYM_ID').is_in(security_non_13f_set) &
    pl.col('FACTSET_ENTITY_ID').is_in(non_holder_13f_set)
    )


# Keep only the most recent 'AS_OF_DATE' observation within quarter
stakes_positions = ( 
    stakes_positions
    .group_by(['FSYM_ID', 'FACTSET_ENTITY_ID', 'date_q'])
    .agg(pl.all().sort_by('AS_OF_DATE').last())
    )



# Re-order and keep cols
stakes_positions = ( 
                    stakes_positions
                    .select(['FSYM_ID',
                            'FACTSET_ENTITY_ID',
                            'AS_OF_DATE',
                            'POSITION',
                            'date_q'])
                    .drop_nulls()
                    )

# Rename
stakes_positions = stakes_positions.rename({'POSITION' : 'ADJ_SHARES_HELD_STAKES'})




# -----------------------------------------------------
#           MERGE WITH MASTER DATAFRAME
# -----------------------------------------------------


scheme_3 = scheme_3.join(stakes_positions, how='left',
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
    own_fund = own_fund.filter(pl.col('REPORTED_HOLDING')>0)
    
    
    # Merge Fund with Institution that manages the Fund
    own_fund = own_fund.join(own_ent_funds, 
                             how='inner',
                             on='FACTSET_FUND_ID')
    

    # Filter 
    own_fund_ =  own_fund.filter(
        pl.col('FSYM_ID').is_in(security_non_13f_set) &
        pl.col('FACTSET_ENTITY_ID').is_in(non_holder_13f_set)
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
    
    
    own_fund_ = own_fund_.select(['FSYM_ID',
                                  'FACTSET_ENTITY_ID',
                                  'date_q',
                                  'REPORT_DATE',
                                  'ADJ_HOLDING'])
       
    
    
    # Sum the position of an institution for each security within a quarter
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
    )     

# Zero adjusted positions are null
funds_positions = funds_positions.with_columns(
                        pl.when(pl.col('ADJ_HOLDING')==0)
                        .then(None)
                        .otherwise('ADJ_HOLDING')
                        .alias('ADJ_HOLDING')
                        )

# Drop null values of adjusted positions
funds_positions = funds_positions.drop_nulls(['ADJ_HOLDING'])
# Rename
funds_positions = funds_positions.rename({'ADJ_HOLDING' : 'ADJ_SHARES_HELD_FUNDS'})



# -----------------------------------------------------
#           MERGE WITH MASTER DATAFRAME
# -----------------------------------------------------

scheme_3 = scheme_3.join(funds_positions, how='left',
                         on=['FSYM_ID', 'FACTSET_ENTITY_ID', 'date_q'])


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#  WINDOW OF 18 MONTHS FOR NA SECURITIES and 21 MONTHS FOR GLOBAL SECURITIES
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Compare the perspective date to the as_of_date and forward fill null positions
# using the following windows:
# o North-American traded securities (US & Canada): 18 months or 6 quarters
# o Global securities: 21 months or 7 quarters


# I need to augment with iso_country of each security using own_sec_coverage
# table
iso_country = pl.read_parquet(os.path.join(factset_dir, 'own_sec_coverage_eq.parquet'),
                               use_pyarrow=True, columns =['FSYM_ID', 'ISO_COUNTRY'])

scheme_3 = scheme_3.join(iso_country, how='left', on=['FSYM_ID'])

# Classify stocks into NA securities and Global securities
scheme_3 = scheme_3.with_columns(
    pl.when(pl.col('ISO_COUNTRY').is_in(set(['US', 'CA'])))
    .then(1)
    .otherwise(0)
    .alias('IS_NA_SECURITY')
    )

# Split the dataset into NA and Global securities,
# apply the forward filling of up to 6 or 7 quarters
# in each dataset separately and then merge them.
# There must be a cleaner way to do this.

# For NA securities fill position 6 quarters ahead
scheme_3_na = ( 
    scheme_3
    .filter(pl.col('IS_NA_SECURITY')==1)
    .with_columns(
                pl.col('ADJ_SHARES_HELD_STAKES')
                .forward_fill(limit=6)
                .over(['FSYM_ID', 'FACTSET_ENTITY_ID'])
                .alias('ADJ_SHARES_HELD_STAKES_FILLED')
                )
    )

# For Global securities fill position 7 quarters ahead
scheme_3_global = (
    scheme_3
    .filter(pl.col('IS_NA_SECURITY')==0)
    .with_columns(
                pl.col('ADJ_SHARES_HELD_STAKES')
                .forward_fill(limit=7)
                .over(['FSYM_ID', 'FACTSET_ENTITY_ID'])
                .alias('ADJ_SHARES_HELD_STAKES_FILLED')
                )
    )
            

# Concat the NA and Global securities into one dataframe again
scheme_3 = pl.concat([scheme_3_na, scheme_3_global])

# Free memory
del scheme_3_na, scheme_3_global, iso_country

# Use a filled stakes position if it exists.
# Otherwise use a funds position.
scheme_3_final = ( 
    scheme_3.with_columns(
    pl.when(pl.col('ADJ_SHARES_HELD_STAKES_FILLED').is_not_null())
    .then(pl.col('ADJ_SHARES_HELD_STAKES_FILLED'))
    .otherwise(pl.col('ADJ_SHARES_HELD_FUNDS'))
    .alias('ADJ_SHARES_HELD')
    )
    )
    
    
# ~~~~~~~~~~~~~~~~~~~~~~   
#    SOME HOUSEKEEPING
# ~~~~~~~~~~~~~~~~~~~~~~

# Select columns and drop null positions
scheme_3_final = (
    scheme_3_final
    .select(['FSYM_ID', 'FACTSET_ENTITY_ID', 'date_q', 'ADJ_SHARES_HELD' ])
    .drop_nulls()
    )


# Define the scheme
scheme_3_final = scheme_3_final.with_columns(
   pl.lit(3).alias('SCHEME')               
    ) 

# Free memory
del scheme_3, funds_positions, stakes_positions, own_fund, own_fund_, own_fund_inst


# ~~~~~~~~~~~~~~
#   SAVE
# ~~~~~~~~~~~
scheme_3_final.write_parquet(os.path.join(cd, 'scheme_3_adj_shares_held.parquet'))


