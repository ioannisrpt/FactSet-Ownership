# -*- coding: utf-8 -*-
"""
Active share as per Koijen, Richmond & Yogo (2023)

I use the definition of Koijen, Richmond & Yogo (2023) to
calculate the active share of Cremers & Petajisto (2009).

For each investment advisor i, I calculate its active share as the
sum of absolute deviations of an investor’s portfolio from a market-weighted 
portfolio, based on the same securities as the ones held by the investor, 
divided by two:
    
AS_it = 0.5 * Sum_{j=1}^{N_it} | w_ijt - w_imt |

for institution i in quarter t. w_ijt is the weight that security j
has in the portfolio of investor i in quarter t and w_imt is the hypothetical
market cap weight of security j if investors held the market-cap portfolio
of his holdings.

Notes:
    i. The active share's definition of Koijen, Richmond & Yogo (2023)
    provides flexibility to calcualte the variable at the institutional 
    level instead of the fund level as in Cremers & Petajisto (2009).
    


Cremers, K. M., & Petajisto, A. (2009). How active is your fund manager?
A new measure that predicts performance. 
The review of financial studies, 22(9), 3329-3365.
 
Koijen, R. S., Richmond, R. J., & Yogo, M. (2023). 
Which investors matter for equity valuations and expected returns?.
Review of Economic Studies, rdad083.


Input:
    Factset\factset_mcap_holdings_company_level.parquet 
    
Output:
    Factset\active_share_koijen2023.parquet


"""


import os
import polars as pl



wdir = r'C:\Users\ropot\Desktop\Python Scripts\Role of institutional investors on integration'
os.chdir(wdir)

# Set up environment
import env



# ~~~~~~~~~~~~
# IMPORT DATA
# ~~~~~~~~~~~~


# Factset market cap holdings at the company level
fh = pl.read_parquet(env.factset_dir('factset_mcap_holdings_company_level.parquet'))


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#   TOTAL MARKET CAP HOLDINGS PER INSTITUTION-QUARTER
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


fh = fh.with_columns(
    pl.col('MCAP_HELD')
    .sum()
    .over(['FACTSET_ENTITY_ID', 'date_q'])
    .alias('MCAP_HELD_TOTAL')
    )


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#   PORTFOLIO WEIGHTS PER SECURITY-INSTITUTION-QUARTER
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


fh = fh.with_columns(
    (pl.col('MCAP_HELD') / pl.col('MCAP_HELD_TOTAL')).alias('INST_PORTFOLIO_WEIGHT')
    )


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#  TOTAL MARKET CAP OF SECURITIES PER INSTITUTION-QUARTER
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


fh = fh.with_columns(
    pl.col('MCAP_TOTAL')
    .sum()
    .over(['FACTSET_ENTITY_ID', 'date_q'])
    .alias('INST_MCAP_TOTAL')
    )




# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#  MARKET CAP WEIGHTS PER SECURITY-INSTITUION-QUARTER
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

fh = fh.with_columns(
    (pl.col('MCAP_TOTAL') / pl.col('INST_MCAP_TOTAL')).alias('MCAP_PORTFOLIO_WEIGHT')
    )


# ~~~~~~~~~~~~~~~~~
#   ACTIVE SHARE
# ~~~~~~~~~~~~~~~~~


# Isolate only the necessary columns
fh_ = fh.select(['FACTSET_ENTITY_ID_FROM_FSYM_ID',
                 'FACTSET_ENTITY_ID',
                 'date_q',
                 'INST_PORTFOLIO_WEIGHT',
                 'MCAP_PORTFOLIO_WEIGHT'])

# Sanity check: do weights sum to 1?
norm = ( 
        fh_
        .group_by(['FACTSET_ENTITY_ID', 'date_q'])
        .agg(pl.col('INST_PORTFOLIO_WEIGHT').sum(),
             pl.col('MCAP_PORTFOLIO_WEIGHT').sum())
        .sort(['FACTSET_ENTITY_ID', 'date_q'])
        )


# Definition of active share
fh_ = fh_.with_columns(
    (pl.col('INST_PORTFOLIO_WEIGHT') - pl.col('MCAP_PORTFOLIO_WEIGHT'))
    .abs()
    .alias('ABS_DEVIATION')
    )

active_share = (
    fh_
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

active_share.write_parquet(env.factset_dir('active_share_koijen2023.parquet'))


        

