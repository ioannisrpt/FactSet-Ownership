# -*- coding: utf-8 -*-
"""
Factset methodology for institutional ownership calculations


Users can choose the order in which the sources are selected, 
if there is more than one source available for a 
given report date.


2. For 13F Holder + 13F CA Securities - Scheme 2
--------------------------------------

This logic should be used if the holder is classified as a 13F filer by FactSet
(own_ent_institutions.fds_13f_flag=1) and 
if the security is classified as a Canadian 13F reportable
by FactSet (own_sec_coverage.fds_13f_ca_flag=1)

• Use the latest 13F position for the security+holder combination, 
unless there is a more recent
stakes-based position in own_inst_stakes_detail
• If there is no 13F position, and no stakes-based position, 
then sum of funds positions should
be used if available.

Position is in terms of market capitalization.

Output:
    scheme_2_mcap_held.parquet
  
    
entity_id  fsym_id   date_q    hcap(diego)  hcap(ioannis) absolute difference
002HL1-E  G6VGLX-S  201003  8.480000e+06  9503897.0  1.023897e+06


"""


import os
import polars as pl


def any_duplicates(df, unique_cols):
    a = df.shape[0]
    b = df.unique(unique_cols).shape[0]
    print('Before unique: %d \n' % a )

    print('After unique: %d \n' %  b )

    print('Difference: %d' %(a-b))


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

#    For 13F Holder and 13F CA Securities  - SCHEME 2

# ///////////////////////////////////////////////////////

print('13F Holder + 13F CA Securities - SCHEME 2 (MCAP HELD) \n')



# 13F holder
holder_13f = own_ent_inst.filter(pl.col('FDS_13F_FLAG') == 1) 
holder_13f_set = set(holder_13f['FACTSET_ENTITY_ID'])

# 13F Canadian security
security_ca_13f = own_sec_cov.filter(pl.col('FDS_13F_CA_FLAG') == 1) 
security_ca_13f_set = set(security_ca_13f['FSYM_ID'])
    
# Define dataframe to save 
scheme_2 = pl.DataFrame()


# ~~~~~~~~~~~~~~~~~~~~
#      13F table
# ~~~~~~~~~~~~~~~~~~~~


# Iterate through 13f datasets
for dataset in os.listdir(own_inst_13f_dir):
    
    if '13f' in dataset:
    
        # Import 13f dataset
        own_inst_13f = pl.read_parquet(os.path.join(own_inst_13f_dir, dataset),
                                       use_pyarrow=True)
        
        # Keep only positive positions
        own_inst_13f = own_inst_13f.filter(pl.col('REPORTED_HOLDING')>0)
        
        # Adjusted holdings of 0 are considered null
        own_inst_13f = own_inst_13f.with_columns(
            pl.when(pl.col('ADJ_HOLDING') == 0)
            .then(None)
            .otherwise(pl.col('ADJ_HOLDING'))
            .alias('ADJ_HOLDING')
            )
        
        # Filter 
        inst_13f_ =  own_inst_13f.filter(
            pl.col('FSYM_ID').is_in(security_ca_13f_set) &
            pl.col('FACTSET_ENTITY_ID').is_in(holder_13f_set)
            )
        
        inst_13f_ = inst_13f_.select(['FSYM_ID',
                                      'FACTSET_ENTITY_ID',
                                      'REPORT_DATE',
                                      'ADJ_HOLDING',
                                      'ADJ_MV',
                                      'REPORTED_HOLDING'])
        
        # Define quarter 'date_q' in integer format based on 'REPORT_DATE'
        inst_13f_ = apply_quarter_scheme(inst_13f_, 'REPORT_DATE')
        
        # Concat 
        scheme_2 = pl.concat([scheme_2, inst_13f_])
    



# Free memory
del own_inst_13f
del inst_13f_


"""
scheme_2.filter( (pl.col('FACTSET_ENTITY_ID') == '002HL1-E') & 
                (pl.col('FSYM_ID') == 'G6VGLX-S' ) & 
                (pl.col('date_q') == 201003) )
"""

# ----------------------
# AUGMENT WITH MARKET CAP
# ----------------------

# Join with prices
scheme_2 = scheme_2.join(prices_q, 
                         how='left',
                         on=['FSYM_ID', 'date_q'])

# Market cap of holdings
scheme_2 = scheme_2.with_columns(
    (pl.col('ADJ_HOLDING') * pl.col('ADJ_PRICE')).alias('MCAP_HELD_FROM_ADJ'),
    (pl.col('REPORTED_HOLDING') * pl.col('UNADJ_PRICE')).alias('MCAP_HELD_FROM_UNADJ')
    )

# Use market cap from adjusted positions unless it is missing. then use
# market cap from reported positions.
scheme_2 = scheme_2.with_columns(
    pl.when(pl.col('MCAP_HELD_FROM_ADJ').is_not_null())
    .then(pl.col('MCAP_HELD_FROM_ADJ'))
    .otherwise(pl.col('MCAP_HELD_FROM_UNADJ'))
    .alias('MCAP_HELD')
    )

# Keep necessary cols and drop rows where market cap holdings are missing
scheme_2 = ( 
            scheme_2
            .select(['FSYM_ID',
                    'FACTSET_ENTITY_ID',
                    'date_q',
                    'REPORT_DATE',
                    'MCAP_HELD'])
            .drop_nulls(['MCAP_HELD'])
            )

# ~~~~~~~~~~~~~~~~~~~~
#   STAKES table
# ~~~~~~~~~~~~~~~~~~~~


# Is there a most recent position in own_inst_stakes_detail within a 
# quarter 'date_q'?
# -------------------------------------------------------------------

# Filter for 13f Canadian security +  13f holder 
own_inst_stakes_ =  own_inst_stakes.filter(
    pl.col('FSYM_ID').is_in(security_ca_13f_set) &
    pl.col('FACTSET_ENTITY_ID').is_in(holder_13f_set)
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
              
# Keep only the most recent 'AS_OF_DATE' observation within quarter
own_inst_stakes_ = ( 
    own_inst_stakes_
    .group_by(['FSYM_ID', 'FACTSET_ENTITY_ID', 'date_q'])
    .agg(pl.all().sort_by('AS_OF_DATE').last())
    )



# -------------------------
# AUGMENT WITH MARKET CAP
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
own_inst_stakes_ = (
    own_inst_stakes_
    .select(['FSYM_ID', 
             'FACTSET_ENTITY_ID',
             'date_q',
             'AS_OF_DATE',
             'MCAP_HELD_STAKES'])
    .drop_nulls(['MCAP_HELD_STAKES'])
    )


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#   MERGE 13F WITH STAKES HOLDINGS
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
              
# Outer join 13F hodlings with stakes holdings on holder-security-quarter 
# I opt for outer join instead of left join because there might be
# holder-security positions that 13F do not capture.
scheme_2 = scheme_2.rename({'MCAP_HELD' : 'MCAP_HELD_13F'})

scheme_2_ = scheme_2.join(own_inst_stakes_, how='outer_coalesce',
                         on=['FSYM_ID', 'FACTSET_ENTITY_ID', 'date_q'])
              
              
# Keep the position that is most recent over holder-security-quarter 
scheme_2_adj_stakes = ( 
                scheme_2_
              .with_columns(
               pl.when(pl.col('AS_OF_DATE') > pl.col('REPORT_DATE'))
               .then(pl.col('MCAP_HELD_STAKES'))
               .otherwise(pl.col('MCAP_HELD_13F'))
               .over(['FSYM_ID', 'FACTSET_ENTITY_ID', 'date_q'])
               .alias('MCAP_HELD')
               )
              )

# Keep the stakes position for security-holder-quarter if there is no
# 13F position.
scheme_2_adj_stakes = (
    scheme_2_adj_stakes
    .with_columns(
    pl.when(pl.col('MCAP_HELD').is_null())
    .then(pl.col('MCAP_HELD_STAKES'))
    .otherwise(pl.col('MCAP_HELD'))
    .alias('MCAP_HELD')
    )
    )


# Finally, we keep only unique observations and select only the relevant
# columns.
scheme_2_adj_stakes = (
                    scheme_2_adj_stakes
                    .unique(['FSYM_ID',
                             'FACTSET_ENTITY_ID',
                             'date_q',
                             'MCAP_HELD'])
                    .select(['FSYM_ID',
                             'FACTSET_ENTITY_ID',
                             'date_q',
                             'MCAP_HELD'])
                    .drop_nulls()
                    )

# Free memory
del scheme_2_, scheme_2, own_inst_stakes_


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#      FUNDS table
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# If there is no 13F position, and no stakes-based position, 
# then sum of funds positions should be used if available.
# ---------------------------------------------------------


# Isolate holder-security pairs that did not have a 13F or 
# stakes based position


# Define DataFrame to store
scheme_2_funds = pl.DataFrame()
#dataset = 'own_fund_detail_eq_1.parquet' 

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
    own_fund_ =  own_fund.filter(
        (pl.col('FSYM_ID').is_in(security_ca_13f_set)) &
        (pl.col('FACTSET_ENTITY_ID').is_in(holder_13f_set))
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
        pl.when(pl.col('ADJ_MV').is_not_null() & pl.col('ADJ_MV') > 0)
        .then(pl.col('ADJ_MV'))
        .otherwise(pl.col('REPORTED_MV'))
        .alias('MCAP_HELD')
        )
    
    own_fund_ = own_fund_.select(['FSYM_ID',
                                  'FACTSET_ENTITY_ID',
                                  'REPORT_DATE',
                                  'MCAP_HELD',
                                  'date_q'])

    

    
    # Sum the position of an institution for each security within a quarter
    own_fund_inst = (
        own_fund_
        .group_by(['FSYM_ID', 'FACTSET_ENTITY_ID', 'date_q'])
        .agg(pl.col('MCAP_HELD').sum())
        )
    
    
    # Concat 
    scheme_2_funds = pl.concat([scheme_2_funds, own_fund_inst])
    
    
# Sum positions again because a security in a quarter that belongs to a different
# fund  under the same institution might appear in a different own_fund_eq table
scheme_2_funds = ( 
    scheme_2_funds
    .group_by(['FSYM_ID', 'FACTSET_ENTITY_ID', 'date_q'])
    .agg(pl.col('MCAP_HELD').sum())
    )  



# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#   MERGE 13F + STAKES WITH SUM OF FUNDS HOLDINGS
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Rename so I can differentiate the source of the position
scheme_2_adj_stakes = scheme_2_adj_stakes.rename({'MCAP_HELD' : 'MCAP_HELD_13F_STAKES'})

scheme_2_funds = scheme_2_funds.rename({'MCAP_HELD' : 'MCAP_HELD_FUNDS'})


# Outer join the sum of funds dataset with institution-security-quarter pair
scheme_2_final = scheme_2_adj_stakes.join(scheme_2_funds, 
                                          how='outer_coalesce',
                                          on=['FACTSET_ENTITY_ID', 'FSYM_ID', 'date_q'])


# Calculate the ultimate position 'MCAP_HELD' as:
# If 'MCAP_HELD_13F_STAKES' is not missing, then use 
# 'MCAP_HELD_13F_STAKES'  for the position.
# Otherwise, use the sum of funds position 'MCAP_HELD_FUNDS'
scheme_2_final = scheme_2_final.with_columns(
    pl.when(pl.col('MCAP_HELD_13F_STAKES').is_not_null())
    .then(pl.col('MCAP_HELD_13F_STAKES'))
    .otherwise(pl.col('MCAP_HELD_FUNDS'))
    .alias('MCAP_HELD')
    )


# Keep only the rows where 'MCAP_HELD' is not null
scheme_2_final = scheme_2_final.drop_nulls(['MCAP_HELD'])

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#      SOME HOUSEKEEPING
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Keep only necessary columns
scheme_2_final = scheme_2_final.select(['FSYM_ID',
                                        'FACTSET_ENTITY_ID',
                                        'date_q',
                                        'MCAP_HELD'])

# Sort
scheme_2_final = scheme_2_final.sort(by=['FSYM_ID', 'FACTSET_ENTITY_ID', 'date_q'])



# Define the scheme
scheme_2_final = scheme_2_final.with_columns(
   pl.lit(2).alias('SCHEME')               
    ) 

# Free memory
del scheme_2_adj_stakes,  scheme_2_funds, own_fund_inst, own_fund_, own_fund



# ~~~~~~~~~~~~~~
#   SAVE
# ~~~~~~~~~~~


scheme_2_final.write_parquet(os.path.join(cd, 'scheme_2_mcap_held.parquet'))



    