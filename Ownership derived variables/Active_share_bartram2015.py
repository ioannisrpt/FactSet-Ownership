# -*- coding: utf-8 -*-
"""
Active share as per Bartram et al. (2015)

I use the definition of Bartram et al. (2015) to
calculate the active share of Cremers & Petajisto (2009).

For each investment advisor i, I calculate its active share as the
sum of absolute deviations of an investor’s portfolio from a market-weighted 
local, regional or global portfolio, based on the classification of an investor
as local, regional or global. The deviations are deviated by two. 

    
AS_it = 0.5 * Sum_{j=1}^{N_it} | w_ijt - w_imt |

for institution i in quarter t. w_ijt is the weight that security j
has in the portfolio of investor i in quarter t and w_imt is the hypothetical
market cap weight of security j if investors held a local, regional or global
market-cap portfolio based on their classification.

Notes:
    i. The active share's definition of Bartram et al. (2015)
    provides flexibility to calculate the variable at the institutional 
    level instead of the fund level as in Cremers & Petajisto (2009).
    


Cremers, K. M., & Petajisto, A. (2009). How active is your fund manager?
A new measure that predicts performance. 
The review of financial studies, 22(9), 3329-3365.

Bartram, S. M., Griffin, J. M., Lim, T. H., & Ng, D. T. (2015). 
How important are foreign ownership linkages for international stock returns?. 
The Review of Financial Studies, 28(11), 3036-3072.



Input:
    holdingsall_company_level.parquet 
    investors_type.parquet
    
    
Output:
    active_share_bartram2015.parquet


"""


import os
import polars as pl



# Current directory
cd = r'C:\Users\FMCC\Desktop\Ioannis'


# Parquet Factset tables
factset_dir =  r'C:\FactSet_Downloadfiles\zips\parquet'


# ~~~~~~~~~~~~
# IMPORT DATA
# ~~~~~~~~~~~~


# Factset market cap holdings at the security level
fh = pl.read_parquet(os.path.join(cd,'holdingsall_company_level.parquet'))

# Classification of investors/institutions to local, regional or global
investors_type = pl.read_parquet(os.path.join(cd, 'investors_type.parquet'))

# sym entity table : country origin of entities
sym_entity =  pl.read_parquet(os.path.join(factset_dir, 'sym_entity.parquet'),
                           use_pyarrow=True, 
                           columns=['FACTSET_ENTITY_ID', 
                                    'ISO_COUNTRY'])
sym_entity = sym_entity.rename({'FACTSET_ENTITY_ID' : 'COMPANY_ID'})

# ISO country and Region match
iso_region = pl.read_csv(os.path.join(cd, 'iso_region_match.csv'))

# Market capitalization of all companies in the investable universe
hmktcap = pl.read_parquet(os.path.join(cd, 'hmktcap.parquet'))
hmktcap = hmktcap.rename({'FACTSET_ENTITY_ID' : 'COMPANY_ID'})


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#   COMPANY LEVEL HOLDINGS
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

fh = (
      fh
      .group_by(['FACTSET_ENTITY_ID', 'COMPANY_ID', 'date_q'])
      .agg(pl.col('IO').sum(),
           pl.col('MKTCAP_HELD').sum(),
           pl.col('MKTCAP_USD').last())
      )

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#   WHAT MATTERS IS THE COUNTRY OF ORIGIN OF COMPANY
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

iso_company = sym_entity.join(iso_region,
                              how='left',
                              on=['ISO_COUNTRY'])

# Companies with holdings
own_companies = list(fh.select(['COMPANY_ID']).unique()['COMPANY_ID'])

iso_company = ( 
    iso_company
    .filter(pl.col('COMPANY_ID').is_in(own_companies))
    .rename({'ISO_COUNTRY' : 'COUNTRY'})
    )

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#  AUGMENT HOLDINGS WITH CLASSIFICATION AND COMPANY COUNTRY
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Keep only classified investors/institutions
investors_type = investors_type.filter(pl.col('IS_CLASSIFIED')==1) 

# Augment
fh_ = fh.join(investors_type.drop(['IS_CLASSIFIED']),
              how='inner',
              on=['FACTSET_ENTITY_ID', 'date_q'])

fh_ = fh_.join(iso_company.drop(['COUNTRY_NAME']),
               how='inner',
               on=['COMPANY_ID'])



# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#  INSTITUTIONAL PORTFOLIO WEIGHTS PER COMPANY-INSTITUTION-QUARTER
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Total market cap holdings per institution-quarter
fh_ = fh_.with_columns(
    pl.col('MKTCAP_HELD')
    .sum()
    .over(['FACTSET_ENTITY_ID', 'date_q'])
    .alias('MKTCAP_HELD_TOTAL')
    )

# Institutional portfolio weights
fh_ = fh_.with_columns(
    (pl.col('MKTCAP_HELD') / pl.col('MKTCAP_HELD_TOTAL')).alias('INST_PORTFOLIO_WEIGHT')
    )



# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#  MARKET CAP LOCAL, REGIONAL, GLOBAL PORTFOLIO WEIGHTS PER COMPANY-QUARTER 
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


# For local institutions, market cap portfolio weights are calculated from 
# the value-weighted local market portfolio
# For regional institutions, market cap portfolio weights are calculated from
# the value-weighted regional market portfolio
# For global institutions, market cap portfolio weights are calculated from
# the value-weighted global market portfolio (all investable companies)

# Augment company information with country and region
hmktcap = hmktcap.join(sym_entity, how='left', on=['COMPANY_ID'])
hmktcap = hmktcap.join(iso_region, how='left', on=['ISO_COUNTRY'])
hmktcap = ( 
    hmktcap
    .drop(['COUNTRY_NAME'])
    .rename({'ISO_COUNTRY' : 'COUNTRY'})
    )

# Use a different dataframe
mcap = hmktcap

# Total market capitalization per country 
mcap = mcap.with_columns(
    pl.col('MKTCAP_USD').sum()
    .over(['COUNTRY', 'date_q'])
    .alias('MKTCAP_TOTAL_COUNTRY')
    )

# Total market capitalization per region
mcap = mcap.with_columns(
    pl.col('MKTCAP_USD').sum()
    .over(['REGION', 'date_q'])
    .alias('MKTCAP_TOTAL_REGION')
    )

# Total market capitalization of the world
mcap = mcap.with_columns(
    pl.col('MKTCAP_USD').sum()
    .over(['date_q'])
    .alias('MKTCAP_TOTAL_WORLD')
    )

# Market-cap weighted portfolio weights per country, region, world
mcap = mcap.with_columns(
    (pl.col('MKTCAP_USD')/pl.col('MKTCAP_TOTAL_COUNTRY')).alias('LOCAL_MARKET_PORTFOLIO_WEIGHT'),
    (pl.col('MKTCAP_USD')/pl.col('MKTCAP_TOTAL_REGION')).alias('REGIONAL_MARKET_PORTFOLIO_WEIGHT'),
    (pl.col('MKTCAP_USD')/pl.col('MKTCAP_TOTAL_WORLD')).alias('WORLD_MARKET_PORTFOLIO_WEIGHT') 
    )

# Keep necessary columns
market_portfolio_weights = mcap.select(['COMPANY_ID',
                                      'date_q',
                                      'COUNTRY',
                                      'REGION',
                                      'LOCAL_MARKET_PORTFOLIO_WEIGHT',
                                      'REGIONAL_MARKET_PORTFOLIO_WEIGHT',
                                      'WORLD_MARKET_PORTFOLIO_WEIGHT'])



# ~~~~~~~~~~~~~~~~~
#   ACTIVE SHARE
# ~~~~~~~~~~~~~~~~~


# Isolate necessary columns
inst_portfolio_weights = fh_.select(['FACTSET_ENTITY_ID',
                                    'COMPANY_ID',
                                    'date_q',
                                    'COUNTRY_MAX',
                                    'REGION_MAX',
                                    'IS_LOCAL_INVESTOR',
                                    'IS_REGIONAL_INVESTOR',
                                    'IS_GLOBAL_INVESTOR',
                                    'INST_PORTFOLIO_WEIGHT'])



# Free memory 
del fh, fh_

# Augment with market cap portfolio weights
portfolio_weights = inst_portfolio_weights.join(market_portfolio_weights,
                                              how='left',
                                              on=['COMPANY_ID', 'date_q'])



# Free memory
del inst_portfolio_weights, market_portfolio_weights


# For local investors, use local market portfolio weight
# For regional investors, use regional market portfolio weight
# For global investors, use global market portfolio weight
portfolio_weights = portfolio_weights.with_columns(
    pl.when((pl.col('IS_LOCAL_INVESTOR')==1) & (pl.col('COUNTRY') == pl.col('COUNTRY_MAX')))
    .then((pl.col('INST_PORTFOLIO_WEIGHT') - pl.col('LOCAL_MARKET_PORTFOLIO_WEIGHT')).abs())
    .when((pl.col('IS_REGIONAL_INVESTOR') == 1) & (pl.col('REGION') == pl.col('REGION_MAX')))
    .then((pl.col('INST_PORTFOLIO_WEIGHT') - pl.col('REGIONAL_MARKET_PORTFOLIO_WEIGHT')).abs())
    .when(pl.col('IS_GLOBAL_INVESTOR') == 1)
    .then((pl.col('INST_PORTFOLIO_WEIGHT') - pl.col('WORLD_MARKET_PORTFOLIO_WEIGHT')).abs())
    .alias('ABS_DEVIATION')
    )




active_share = (
    portfolio_weights
    .group_by(['FACTSET_ENTITY_ID', 'date_q'])
    .agg((0.5*pl.col('ABS_DEVIATION').sum()).alias('ACTIVE_SHARE'))
    .sort(['FACTSET_ENTITY_ID', 'date_q'])
    )



# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#   QUARTERLY MEDIAN OF ACTIVE SHARE
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

median_point = ( 
                active_share
                .group_by('date_q')
                .agg(pl.col('ACTIVE_SHARE').quantile(0.5).alias('ACTIVE_SHARE_MEDIAN'))
                .sort(by='date_q')
                )


# Augment main dataset
active_share = active_share.join(median_point,
                                 how='left', 
                                 on ='date_q')


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#   PASSIVE VS ACTIVE INVESTORS
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Classification
active_share = active_share.with_columns(
    pl.when(pl.col('ACTIVE_SHARE') <= pl.col('ACTIVE_SHARE_MEDIAN'))
    .then(1)
    .otherwise(0)
    .alias('IS_PASSIVE_INVESTOR')
    )


# ~~~~~~~~~~~
#  SAVE
# ~~~~~~~~~

active_share = active_share.drop(['ACTIVE_SHARE_MEDIAN'])

active_share.write_parquet(os.path.join(cd, 'active_share_bartram2015.parquet'))



