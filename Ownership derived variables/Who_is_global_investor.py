r"""
Who is a global investor?


I use the definition of Bartram et al. (2015) to define global investors but 
instead of using a 12-quarter window I use a 4-quarter or one year window:
    
"We calculate for each fund the percentage of its holdings that are in a country 
and a region in a quarter. 

If the maximum average percentage of the holdings 
in a country over the previous twelve quarters is more than 90% of
 the fund's total holdings, the fund is classified as a country fund. 
 
 Otherwise, 
 if the maximum average percentage in a region is more than 80%, it is a region 
 fund.

 Otherwise, it is a global fund."


The regions are:
    1. North America (NA)
    2. South America (SA)
    3. Europe and UK (EU)
    4. Middle East (ME)
    5. Africa (Africa)
    6. Asia (Asia)
    7. Oceania
    


Bartram, S. M., Griffin, J. M., Lim, T. H., & Ng, D. T. (2015). 
How important are foreign ownership linkages for international stock returns?. 
The Review of Financial Studies, 28(11), 3036-3072.


Notes:
    1. Do I treat missing lagged positions of institutions as 0?
    It is a tricky question with significant consequences in the calculation.

Input:
   \factset_mcap_holdings_company_level.parquet 
   \own_sec_universe.parquet
   ...\iso_region_match.csv
    
Output:
    ...\investors_type.parquet

"""



import os
import polars as pl
import pandas as pd

# ~~~~~~~~~~~~~~
#  DIRECTORIES
# ~~~~~~~~~~~~~~


# Current directory
cd = r'C:\Users\FMCC\Desktop\Ioannis'

# Parquet Factset tables
factset_dir =  r'C:\FactSet_Downloadfiles\zips\parquet'

# Set up environment
#import env


print('Who is a global investor? - START \n')


# ~~~~~~~~~~~~~~~~~
#   IMPORT DATA
# ~~~~~~~~~~~~~~~~~

# Onwership holdings at the company level
fh = pl.read_parquet(os.path.join(cd, 'factset_mcap_holdings_company_level.parquet'))

# Universe of securities in the Ownership bunlde
own_sec_uni = pl.read_parquet(os.path.join(cd, 'own_sec_universe.parquet'),
                              use_pyarrow=True)

# ISO country and Region match
iso_region = pl.read_csv(os.path.join(cd, 'iso_region_match.csv'))


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#   PREPARE HOLDINGS DATA
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~





# Augment with the ISO country of the company/entity
iso_entity = ( 
    own_sec_uni
    .select(['FACTSET_ENTITY_ID', 'ISO_COUNTRY_ENTITY'])
    .unique(['FACTSET_ENTITY_ID', 'ISO_COUNTRY_ENTITY'])
    .drop_nulls()
    .rename({'FACTSET_ENTITY_ID' : 'FACTSET_ENTITY_ID_FROM_FSYM_ID'})
    )

fh = fh.join(iso_entity, how='left', on = ['FACTSET_ENTITY_ID_FROM_FSYM_ID'])

# Make sure that there are no null values
fh_ = fh.drop_nulls()

# Select only the necessary columns
fh_ = fh_.select(['FACTSET_ENTITY_ID',
                 'ISO_COUNTRY_ENTITY',  
                  'date_q',
                  'MCAP_HELD'])

fh_ = fh_.rename({'ISO_COUNTRY_ENTITY' : 'ISO_COUNTRY'})


# Map iso country to a region
fh_ = fh_.join(iso_region, how='left', on=['ISO_COUNTRY'])


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#        MASTER DATASET : INSTITUTION-QUARTER PAIRS
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


# Unique institution-quarter pairs
investors_type = ( 
    fh_
    .select(['FACTSET_ENTITY_ID', 'date_q'])
    .unique()
    .sort(by=['FACTSET_ENTITY_ID', 'date_q'])
    )



# /////////////////////////////////////////////////////////////////////////
#
#                     LOCAL INVESTORS 
# 
# //////////////////////////////////////////////////////////////////////

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#   MCAP HOLDINGS PER COUNTRY
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

fh_country = ( 
    fh_
    .group_by(['FACTSET_ENTITY_ID', 'date_q', 'ISO_COUNTRY'])
    .agg(pl.col('MCAP_HELD').sum())
    )

# Sort
fh_country = fh_country.sort(by=['FACTSET_ENTITY_ID', 'date_q', 'ISO_COUNTRY'])



# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#     AUGMENT MCAP HOLDINGS WITH INTERMEDIATE QUARTER DATES
# ~~~~~~~~~~~~~~~~~~~~~~~~~~

# It can be the case that an institution holds a position in
# Japanese securities for quarters 202303 and 202309. Implicitly the
# position for 202306 is 0. The dataset does not reflect that. 
# Thus I need to account for the 0 holdings of such intermediate quarters.

# Find the min and max quarter for each institution and mcap country holdings
min_max_q = (
    fh_country
    .group_by(['FACTSET_ENTITY_ID', 'ISO_COUNTRY'])
    .agg( 
    pl.col('date_q').min().alias('date_q_min'),
    pl.col('date_q').max().alias('date_q_max')
    )
    )


# For each institution-country pair, I augment the 'date_q' with
# the missing intermediate columns by cross joining
# -------------------------------------------------

# All institution-country pair
inst_country_pairs = min_max_q.select(['FACTSET_ENTITY_ID', 'ISO_COUNTRY'])

# All possible quarter dates
date_range = pd.date_range(start = pd.to_datetime('199303', format='%Y%m'), 
                         end   = pd.to_datetime('202403', format='%Y%m'), 
                         freq  = 'Q')

date_range_int = [int(x.strftime('%Y%m')) for x in date_range]
quarters_pl = pl.DataFrame({'date_q' :date_range_int }).cast(pl.Int32)

# Master dataframe
fh_country_aug = inst_country_pairs.join(quarters_pl, how='cross')

# Merge with min-max dates and filter
fh_country_aug = fh_country_aug.join(min_max_q,
                                     how='inner',
                                     on=['FACTSET_ENTITY_ID', 'ISO_COUNTRY'])

fh_country_aug = fh_country_aug.filter(
                  (pl.col('date_q_min') <= pl.col('date_q'))
                 & (pl.col('date_q') <= pl.col('date_q_max'))
    )

fh_country_aug = fh_country_aug.select(['FACTSET_ENTITY_ID',
                                        'ISO_COUNTRY',
                                        'date_q'])


# Merge the mcap positions
fh_country_mcap = fh_country_aug.join(fh_country, 
                                      how='left',
                                      on=['FACTSET_ENTITY_ID',
                                          'ISO_COUNTRY',
                                          'date_q'])

# Fill with 0 any null MCAP_HELD positions
fh_country_mcap = fh_country_mcap.with_columns(
    pl.col('MCAP_HELD').fill_null(0)
    )


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# N-QUARTER ROLLING MEAN OF MCAP % HOLDINGS PER COUNTRY
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Define the percentage of holdings that country represents 
# to an institution in its portfolio for each quarter
fh_country_perc = fh_country_mcap.with_columns(
                (
                  pl.col('MCAP_HELD') / 
                  (pl.col('MCAP_HELD')
                  .sum()
                  .over(['FACTSET_ENTITY_ID', 'date_q'])
                  )
                  ).alias('MCAP_HELD_PERC')
                  
    )

# Define lagged mcap % holdings up to num_quarters quarters
num_quarters = 4

for i in range(1, num_quarters):
    
    fh_country_perc = fh_country_perc.with_columns(
        pl.col('MCAP_HELD_PERC')
        .shift(i)
        .over(['FACTSET_ENTITY_ID', 'ISO_COUNTRY'])
        .alias('MCAP_HELD_PERC_LAG%d' % i)
        )



# Mean of mcap % holdings over num_quarters
fh_country_perc = fh_country_perc.with_columns(
                (
                (   pl.col('MCAP_HELD_PERC')
                 + pl.col('MCAP_HELD_PERC_LAG1')
                 + pl.col('MCAP_HELD_PERC_LAG2')
                 + pl.col('MCAP_HELD_PERC_LAG3') ) / 4
                ).alias('MCAP_HELD_PERC_ROLL4Q')
    )

# Isolate only the necessary columns
fh_country_perc = fh_country_perc.select(['FACTSET_ENTITY_ID', 
                                          'ISO_COUNTRY',
                                          'date_q',
                                          'MCAP_HELD_PERC_ROLL4Q'])

# Drop null values 
fh_country_perc = fh_country_perc.drop_nulls()

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#   PICK THE MAXIMUM AVERAGE MCAP % HOLDINGS 
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ 

fh_country_perc_max = (
    fh_country_perc
    .group_by(['FACTSET_ENTITY_ID', 'date_q'])
    .agg(pl.col('MCAP_HELD_PERC_ROLL4Q').max().alias('MCAP_COUNTRY_MAX'))
    .drop_nulls()
    )

# Also drop nans
fh_country_perc_max = fh_country_perc_max.filter(pl.col('MCAP_COUNTRY_MAX').is_not_nan())

# ~~~~~~~~~~~~~~~~~~~~~~~
#   DEFINE LOCAL INVESTORS
# ~~~~~~~~~~~~~~~~~~~~~~


fh_country_perc_max = fh_country_perc_max.with_columns(
    pl.when(pl.col('MCAP_COUNTRY_MAX')>=0.9)
    .then(1)
    .otherwise(0)
    .alias('IS_LOCAL_INVESTOR')
    )

local_investors = fh_country_perc_max.select(['FACTSET_ENTITY_ID', 
                                              'date_q',
                                              'IS_LOCAL_INVESTOR'])
    

#  MERGE WITH MASTER DATASET
investors_type = investors_type.join(local_investors, 
                                     how='left',
                                     on=['FACTSET_ENTITY_ID', 'date_q'])



# Free memory
del local_investors, fh_country_perc_max, fh_country_perc, fh_country_mcap
del fh_country_aug, min_max_q, fh_country





# /////////////////////////////////////////////////////////////////////////
#
#                     REGIONAL INVESTORS 
# 
# //////////////////////////////////////////////////////////////////////



# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#   MCAP HOLDINGS PER REGION
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

fh_region = ( 
    fh_
    .group_by(['FACTSET_ENTITY_ID', 'date_q', 'REGION'])
    .agg(pl.col('MCAP_HELD').sum())
    )

# Sort
fh_region = fh_region.sort(by=['FACTSET_ENTITY_ID', 'date_q', 'REGION'])



# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#     AUGMENT MCAP HOLDINGS WITH INTERMEDIATE QUARTER DATES
# ~~~~~~~~~~~~~~~~~~~~~~~~~~

# It can be the case that an institution holds a position in
# Asian securities for quarters 202303 and 202309. Implicitly the
# position for 202306 is 0. The dataset does not reflect that. 
# Thus I need to account for the 0 holdings of such intermediate quarters.

# Find the min and max quarter for each institution and mcap region holdings
min_max_q = (
    fh_region
    .group_by(['FACTSET_ENTITY_ID', 'REGION'])
    .agg( 
    pl.col('date_q').min().alias('date_q_min'),
    pl.col('date_q').max().alias('date_q_max')
    )
    )


# For each institution-region pair, I augment the 'date_q' with
# the missing intermediate columns by cross joining
# -------------------------------------------------

# All institution-region pair
inst_region_pairs = min_max_q.select(['FACTSET_ENTITY_ID', 'REGION'])

# All possible quarter dates
date_range = pd.date_range(start = pd.to_datetime('199303', format='%Y%m'), 
                         end   = pd.to_datetime('202403', format='%Y%m'), 
                         freq  = 'Q')

date_range_int = [int(x.strftime('%Y%m')) for x in date_range]
quarters_pl = pl.DataFrame({'date_q' :date_range_int }).cast(pl.Int32)

# Master dataframe
fh_region_aug = inst_region_pairs.join(quarters_pl, how='cross')

# Merge with min-max dates and filter
fh_region_aug = fh_region_aug.join(min_max_q,
                                     how='inner',
                                     on=['FACTSET_ENTITY_ID', 'REGION'])

fh_region_aug = fh_region_aug.filter(
                  (pl.col('date_q_min') <= pl.col('date_q'))
                 & (pl.col('date_q') <= pl.col('date_q_max'))
                     )

fh_region_aug = fh_region_aug.select(['FACTSET_ENTITY_ID',
                                        'REGION',
                                        'date_q'])


# Merge the mcap positions
fh_region_mcap = fh_region_aug.join(fh_region, 
                                      how='left',
                                      on=['FACTSET_ENTITY_ID',
                                          'REGION',
                                          'date_q'])

# Fill with 0 any null MCAP_HELD positions
fh_region_mcap = fh_region_mcap.with_columns(
    pl.col('MCAP_HELD').fill_null(0)
    )




# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# N-QUARTER ROLLING MEAN OF MCAP % HOLDINGS PER REGION
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Define the percentage of holdings that region represents 
# to an institution in its portfolio for each quarter
fh_region_perc = fh_region_mcap.with_columns(
                (
                  pl.col('MCAP_HELD') / 
                  (pl.col('MCAP_HELD')
                  .sum()
                  .over(['FACTSET_ENTITY_ID', 'date_q'])
                  )
                  ).alias('MCAP_HELD_PERC')
                  
    )

# Define lagged mcap % holdings up to num_quarters quarters
num_quarters = 4

for i in range(1, num_quarters):
    
    fh_region_perc = fh_region_perc.with_columns(
        pl.col('MCAP_HELD_PERC')
        .shift(i)
        .over(['FACTSET_ENTITY_ID', 'REGION'])
        .alias('MCAP_HELD_PERC_LAG%d' % i)
        )


# Mean of mcap % holdings over num_quarters
fh_region_perc = fh_region_perc.with_columns(
                (
                (   pl.col('MCAP_HELD_PERC')
                 + pl.col('MCAP_HELD_PERC_LAG1')
                 + pl.col('MCAP_HELD_PERC_LAG2')
                 + pl.col('MCAP_HELD_PERC_LAG3') ) / 4
                ).alias('MCAP_HELD_PERC_ROLL4Q')
    )

# Isolate only the necessary columns
fh_region_perc = fh_region_perc.select(['FACTSET_ENTITY_ID', 
                                          'REGION',
                                          'date_q',
                                          'MCAP_HELD_PERC_ROLL4Q'])

fh_region_perc = fh_region_perc.drop_nulls()


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#   PICK THE MAXIMUM AVERAGE MCAP % HOLDINGS 
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ 

fh_region_perc_max = (
    fh_region_perc
    .group_by(['FACTSET_ENTITY_ID', 'date_q'])
    .agg(pl.col('MCAP_HELD_PERC_ROLL4Q').max().alias('MCAP_REGION_MAX'))
    .drop_nulls()
    )

# Also drop nans
fh_region_perc_max = fh_region_perc_max.filter(pl.col('MCAP_REGION_MAX').is_not_nan())


# ~~~~~~~~~~~~~~~~~~~~~~~
#   DEFINE REGIONAL INVESTORS
# ~~~~~~~~~~~~~~~~~~~~~~


fh_region_perc_max = fh_region_perc_max.with_columns(
    pl.when(pl.col('MCAP_REGION_MAX')>=0.8)
    .then(1)
    .otherwise(0)
    .alias('IS_REGIONAL_INVESTOR')
    )

regional_investors = fh_region_perc_max.select(['FACTSET_ENTITY_ID',
                                                'date_q', 
                                                'IS_REGIONAL_INVESTOR'])
    

#  MERGE WITH MASTER DATASET
investors_type = investors_type.join(regional_investors, 
                                     how='left',
                                     on=['FACTSET_ENTITY_ID', 'date_q'])

 

# Free memory
del regional_investors, fh_region_perc_max, fh_region_perc, fh_region_mcap
del fh_region_aug, min_max_q, fh_region
        




# /////////////////////////////////////////////////////////////////////////
#
#                 GLOBAL INVESTORS 
# 
# //////////////////////////////////////////////////////////////////////


# Define global investors
investors_type = investors_type.with_columns(
    pl.when( (pl.col('IS_LOCAL_INVESTOR') == 0) 
            & (pl.col('IS_REGIONAL_INVESTOR') == 0))
    .then(1)
    .otherwise(0)
    .alias('IS_GLOBAL_INVESTOR')
    )





# ~~~~~~~~~~~~~~~~~~~~~~~
#   SOME HOUSEKEEPING
# ~~~~~~~~~~~~~~~~~~~~~~~


# If an institution is a local investor, then
# it cannot be a regional investor
investors_type = investors_type.with_columns(
    pl.when(pl.col('IS_LOCAL_INVESTOR') == 1)
    .then(0)
    .otherwise(pl.col('IS_REGIONAL_INVESTOR'))
    .alias('IS_REGIONAL_INVESTOR')
    )


# There are situations, when an institution cannot be classified
# as global due to lack of information:
# i. local investor = null & regional investor = 0
# ii. local investor = 0 & regional investor = null
# iii. local investor = null & regional investor = null 
investors_type = investors_type.with_columns(
    pl.when( (  pl.col('IS_LOCAL_INVESTOR').is_null() & (pl.col('IS_REGIONAL_INVESTOR') == 0)     )
           | ( (pl.col('IS_LOCAL_INVESTOR') ==0)      &  pl.col('IS_REGIONAL_INVESTOR').is_null() )
           | (  pl.col('IS_LOCAL_INVESTOR').is_null() &  pl.col('IS_REGIONAL_INVESTOR').is_null() ) 
           )
    .then(None)
    .otherwise(pl.col('IS_GLOBAL_INVESTOR'))
    .alias('IS_GLOBAL_INVESTOR')
    )


# Is an investor classified?
investors_type = investors_type.with_columns(
                                        (pl.col('IS_LOCAL_INVESTOR') 
                                         + pl.col('IS_REGIONAL_INVESTOR') 
                                         + pl.col('IS_GLOBAL_INVESTOR'))
                                        .alias('IS_CLASSIFIED')
                                        )

investors_type = investors_type.with_columns(
    pl.when(pl.col('IS_CLASSIFIED').is_null())
    .then(0)
    .otherwise(pl.col('IS_CLASSIFIED'))
    .alias('IS_CLASSIFIED')
    )


# ~~~~~~~~~~~~~~~
#   SAVE 
# ~~~~~~~~~~~


#investors_type.write_parquet(env.investors_integration_dir('investors_type.parquet'))
investors_type.write_parquet(os.path.join(cd, 'investors_type.parquet'))


print('Who is a global investor? - END \n')

# Sanity checks 

a = investors_type.filter(pl.col('IS_CLASSIFIED') == 1)

# Number of institutions per category through quarters
a_ = a.select(['date_q', 
               'IS_LOCAL_INVESTOR',
               'IS_REGIONAL_INVESTOR', 
               'IS_GLOBAL_INVESTOR'])

num_inst = a_.group_by('date_q').sum().sort('date_q')

num_inst.to_pandas().set_index('date_q').plot()









