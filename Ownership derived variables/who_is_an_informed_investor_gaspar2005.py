r"""
Who is an informed investor?


I use the definition of the average churn rate of Gaspar, Massa & Matos (2005)
to classify institutions into short- and long-term investors. The short-term
investors are better informed and as such they trade and move prices worldwide
according to Yan & Zhang (2009).


The nominator in churn rate is the sum of aggregate purchases and sales
for each institution-quarter pair.

Gaspar, J. M., Massa, M., & Matos, P. (2005).
 Shareholder investment horizons and the market for corporate control.
 Journal of Financial Economics, 76(1), 135-165.

Input:
   \Factset\factset_adj_shares_holdings_security_level.parquet 

    
Output:
    ...\short_and_long_term_investors_gaspar2005.parquet

"""

import os
import polars as pl
import pandas as pd

# ~~~~~~~~~~~~~~~~~~
#    DIRECTORIES 
# ~~~~~~~~~~~~~~~~~~

# Current directory
cd = r'C:\Users\FMCC\Desktop\Ioannis'

# Parquet Factset tables
factset_dir =  r'C:\FactSet_Downloadfiles\zips\parquet'
#factset_dir = r'C:\Users\ropot\Desktop\Financial Data for Research\FactSet'


print('Short- and long-term investors per Gaspar, Massa & Matos (2005) - START')

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


# ~~~~~~~~~~~~
# IMPORT DATA
# ~~~~~~~~~~~~


# Factset adjusted shares holdings
fh = pl.read_parquet(os.path.join(cd, 'factset_adj_shares_holdings_security_level.parquet'))

# Own sec prices
own_sec_prices = pl.read_parquet(os.path.join(factset_dir, 'own_sec_prices_eq.parquet'))


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#  FORMAT OWN_SEC_PRICES TABLE
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


# Define quarter date 'date_q'
own_sec_prices = apply_quarter_scheme(own_sec_prices, 'PRICE_DATE')


# Keep only the most recent 'price' observation within a quarter
# for each security (data already sorted)
own_sec_prices_q = ( 
    own_sec_prices
    .group_by(['FSYM_ID', 'date_q'])
    .agg(pl.all().sort_by('PRICE_DATE').last())
    )


# Adjusted prices only
adj_prices_q = own_sec_prices_q.select(['FSYM_ID',
                                         'date_q',
                                         'ADJ_PRICE'])
            


# Sort
adj_prices_q = adj_prices_q.sort(by=['FSYM_ID', 'date_q'])


# Keep only positive adjusted prices
adj_prices_q = adj_prices_q.filter(pl.col('ADJ_PRICE')>0)


# Free memory 
del own_sec_prices


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#   FULL HISTORY OF QUARTERS IN OWNERSHIP BUNDLE 
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Last date 'end' is excluded.
date_range = pd.date_range(start = pd.to_datetime('198912', format='%Y%m'), 
                         end   = pd.to_datetime('202403', format='%Y%m'), 
                         freq  = 'Q')

date_range_int = [int(x.strftime('%Y%m')) for x in date_range]
quarters_pl = pl.DataFrame({'date_q' :date_range_int }).cast(pl.Int32)

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#   PRICES WITH INTERMEDIATE QUARTER DATES FOR COMPLETE QUARTER HISTORY
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


# Min and max price quarter dates for securities
min_max_q = (
    adj_prices_q
    .group_by(['FSYM_ID'])
    .agg( 
    pl.col('date_q').min().alias('date_q_min'),
    pl.col('date_q').max().alias('date_q_max')
    )
    )



# Ownership securities 
securities = adj_prices_q.select(['FSYM_ID']).unique()

# Cross join securities with all ownership possible quarter dates
securities_q = securities.join(quarters_pl, how='cross')

# Augment with very first and very last price data quarter
securities_q = securities_q.join(min_max_q,
                                how='left',
                                on=['FSYM_ID']
                                )

# Keep only rows between minimum and maximum quarter of price data of security
securities_q = securities_q.filter(
    (pl.col('date_q_min') <= pl.col('date_q') )
    & (pl.col('date_q') <= pl.col('date_q_max') )
    )

# Left join with adjusted price data -> complete quarter price history
securities_q = securities_q.join(adj_prices_q, 
                                 how='left',
                                 on=['FSYM_ID', 'date_q'])

# Define the price of security lagged by one quarter
securities_q = securities_q.with_columns(
    pl.col('ADJ_PRICE').shift().over(['FSYM_ID']).alias('ADJ_PRICE_LAG1')
    )


# Keep only ordinary and preferred (+GDRADR) equity securities that have holdings
securities_holdings = list(fh['FSYM_ID'].unique())
securities_q_ = securities_q.filter(pl.col('FSYM_ID').is_in(securities_holdings))
    


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#   HOLDINGS WITH INTERMEDIATE QUARTER DATES FOR COMPLETE QUARTER HISTORY
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


# Find the min and max quarter for each security-institution pair
min_max_q = (
    fh
    .group_by(['FSYM_ID', 'FACTSET_ENTITY_ID'])
    .agg( 
    pl.col('date_q').min().alias('date_q_min'),
    pl.col('date_q').max().alias('date_q_max')
    )
    )

# Augment with full price history
s = securities_q_.select(['FSYM_ID',
                          'date_q',
                          'ADJ_PRICE',
                          'ADJ_PRICE_LAG1'])

# Create positions table where security-institution-quarter pairs
# are defined with complete quarter history if a position is missing:
# Institution A owns security B in quarter 201003 and 201009 but not
# 201006. I fill the security-institution-quarter pair history with
# quarters such as 201006.
positions = min_max_q.join(s,
                            how='left', 
                            on=['FSYM_ID'])

# Keep only quarters that fall within 
positions = positions.filter(
    (pl.col('date_q_min') <= pl.col('date_q') )
    & (pl.col('date_q') <= pl.col('date_q_max') )
    )

# Select necessary columns
positions = positions.select(['FSYM_ID',
                              'FACTSET_ENTITY_ID',
                                'date_q',
                                'ADJ_PRICE',
                                'ADJ_PRICE_LAG1'])


# Test operations
a = positions.filter((pl.col('FSYM_ID') == 'K023Z4-S') & 
                 (pl.col('FACTSET_ENTITY_ID') == '000HPH-E'))
a.write_csv(os.path.join(cd, 'a_positions.csv'))


# ---------------------
# AUGMENT WITH HOLDINGS
# ----------------------
fh_prices = positions.join(fh, 
                           how='left', 
                           on=['FSYM_ID', 'FACTSET_ENTITY_ID', 'date_q'])

# Test operations
a = fh_prices.filter((pl.col('FSYM_ID') == 'K023Z4-S') & 
                 (pl.col('FACTSET_ENTITY_ID') == '000HPH-E'))
a.write_csv(os.path.join(cd, 'a_fh_prices.csv'))

# Free memory
del min_max_q, s, securities_q, positions


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#     FORMATTING THE MASTER DATASET
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Lag holdings by one quarter
fh_prices = fh_prices.with_columns(
    pl.col('ADJ_SHARES_HELD')
    .shift()
    .over(['FSYM_ID', 'FACTSET_ENTITY_ID'])
    .alias('ADJ_SHARES_HELD_LAG1')
    )




# Fill with 0 the current and lagged holdings of security-institution pairs 
# in intermediate quarter dates when there is no holdings value
fh_prices = fh_prices.with_columns(
    pl.col('ADJ_SHARES_HELD').fill_null(0),
    pl.col('ADJ_SHARES_HELD_LAG1').fill_null(0)
    )




# Keep only the necessary columns for the calculation of churn rate
fh_prices = fh_prices.select(['FACTSET_ENTITY_ID',
                               'FSYM_ID',
                               'date_q',
                               'ADJ_SHARES_HELD',
                               'ADJ_SHARES_HELD_LAG1',
                               'ADJ_PRICE',
                               'ADJ_PRICE_LAG1'])


#  CHANGE IN HOLDINGS FROM QUARTER TO QUARTER
fh_prices = fh_prices.with_columns(
    (pl.col('ADJ_SHARES_HELD') - pl.col('ADJ_SHARES_HELD_LAG1'))
    .alias('DeltaS')
    )

# DEFINE BUY OR SELL 
fh_prices = fh_prices.with_columns(
    pl.when(pl.col('DeltaS')>0)
    .then(1)
    .otherwise(0)
    .alias('IS_BUY')
    )


# Test operations
a = fh_prices.filter((pl.col('FSYM_ID') == 'K023Z4-S') & 
                 (pl.col('FACTSET_ENTITY_ID') == '000HPH-E'))
a.write_csv(os.path.join(cd, 'a_holdings.csv'))


# KEEP ONLY QUARTERS THAT INSTITUTION ENTERS OR EXITS A POSITION
fh_prices = fh_prices.filter(( pl.col('ADJ_SHARES_HELD') > 0 ) |
                             ( pl.col('ADJ_SHARES_HELD') > 0 ))




# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#   AGGREGATE PURCHASE FOR INSTITUTION-QUARTER
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Keep only the buys 
fh_prices_buy = fh_prices.filter(pl.col('IS_BUY') == 1)

# Aggregatte purchase
agg_purchase = (
    fh_prices_buy
    .group_by(['FACTSET_ENTITY_ID', 'date_q'])
    .agg( (pl.col('DeltaS')*pl.col('ADJ_PRICE')).sum().alias('PURCHASE'))
    .sort(by=['FACTSET_ENTITY_ID', 'date_q'])
    )

# Free memory
del fh_prices_buy


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#   AGGREGATE SALE FOR INSTITUTION-QUARTER
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


# Keep only the sells
fh_prices_sell = fh_prices.filter(pl.col('IS_BUY') == 0)

# Aggregatte sale
agg_sale = (
    fh_prices_sell
    .group_by(['FACTSET_ENTITY_ID', 'date_q'])
    .agg( (pl.col('DeltaS').abs()*pl.col('ADJ_PRICE')).sum().alias('SALE'))
    .sort(by=['FACTSET_ENTITY_ID', 'date_q'])
    )

# Free memory
del fh_prices_sell



# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#   DENOMINATOR OF CHURN RATE VARIABLE
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

#  Market cap holdings
hcap = fh_prices.with_columns(
    (pl.col('ADJ_SHARES_HELD')*pl.col('ADJ_PRICE')).alias('HCAP'),
    (pl.col('ADJ_SHARES_HELD_LAG1')*pl.col('ADJ_PRICE_LAG1')).alias('HCAP_LAG1')    
    )

# Average of market cap holdings
hcap = hcap.with_columns(
    ( 0.5*(pl.col('HCAP') + pl.col('HCAP_LAG1')) ).alias('HCAP_AVG')
    )

# Select cols
hcap = hcap.select(['FACTSET_ENTITY_ID', 'date_q', 'HCAP_AVG'])

# Average share position as denominator
denom = (
    hcap
    .group_by(['FACTSET_ENTITY_ID', 'date_q'])
    .agg(pl.col('HCAP_AVG').sum().alias('AVERAGE_POSITION'))
    )

# Free memory
del hcap 

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#   CHURN RAGE FOR EACH INSTITUTION-QUARTER
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Use 'denom' dataset as reference (that is a choice that I believe makes no
# difference in the outcome)
churn = denom.join(agg_purchase, how='left', on=['FACTSET_ENTITY_ID', 'date_q'])
churn = churn.join(agg_sale, how='left', on=['FACTSET_ENTITY_ID', 'date_q'])
# Fill null values with 0
churn = churn.with_columns(
    pl.col('PURCHASE').fill_null(0),
    pl.col('SALE').fill_null(0)    
    )

# Sort
churn = churn.sort(by=['FACTSET_ENTITY_ID', 'date_q'])

# Drop rows where average position is 0
churn = churn.filter(pl.col('AVERAGE_POSITION')>0)

# Sum of aggregate purchase or sale
churn = churn.with_columns(
    (pl.col('PURCHASE') + pl.col('SALE'))
    .alias('SUM_AGG')
    )

# Churn rate
churn = churn.with_columns(
    (pl.col('SUM_AGG') / pl.col('AVERAGE_POSITION'))
    .alias('CHURN_RATE')
    )



# Define lags of churn rate 
num_quarters = 3

for i in range(1, num_quarters+1):
    churn = churn.with_columns(
        pl.col('CHURN_RATE')
        .shift(i)
        .over(['FACTSET_ENTITY_ID'])
        .alias('CHURN_RATE_LAG%d' % i)
        )

# Average churn rate over 4 quarters
churn = churn.with_columns(
    ((pl.col('CHURN_RATE')
    + pl.col('CHURN_RATE_LAG1')
    + pl.col('CHURN_RATE_LAG2')  
    + pl.col('CHURN_RATE_LAG3') ) / 4)
    .alias('CHURN_RATE_ROLL4Q')
    )


# Keep only average churn rate
churn_rate = ( 
    churn
    .select(['FACTSET_ENTITY_ID', 'date_q', 'CHURN_RATE_ROLL4Q'])
    .drop_nulls()
    )


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#   SHORT- AND LONG-TERM INVESTORS
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


tertile_point_1 = ( 
    churn_rate
    .group_by('date_q')
    .agg(pl.col('CHURN_RATE_ROLL4Q').quantile(0.33).alias('tertile_point_1'))
    .sort(by='date_q')
    )

tertile_point_2  = (
    churn_rate
    .group_by('date_q')
    .agg(pl.col('CHURN_RATE_ROLL4Q').quantile(0.66).alias('tertile_point_2'))
    .sort(by='date_q')
    )


# Augment with tertile breakpoints
term = churn_rate.join(tertile_point_1, how='left', on=['date_q'])
term = term.join(tertile_point_2, how='left', on=['date_q'])


# SHORT-TERM INVESTORS
term = term.with_columns(
    pl.when(pl.col('CHURN_RATE_ROLL4Q') > pl.col('tertile_point_2'))
    .then(1)
    .otherwise(0)
    .alias('IS_SHORT_TERM_INVESTOR')
    )


# LONG-TERM INVESTORS
term = term.with_columns(
    pl.when(pl.col('CHURN_RATE_ROLL4Q') <= pl.col('tertile_point_1'))
    .then(1)
    .otherwise(0)
    .alias('IS_LONG_TERM_INVESTOR')
    )


# ~~~~~~~~~~~~~~~~~
#      SAVE 
# ~~~~~~~~~~~~~~~~~~

term_ = ( 
    term.select(['FACTSET_ENTITY_ID', 
                   'date_q',
                   'IS_SHORT_TERM_INVESTOR',
                   'IS_LONG_TERM_INVESTOR'])
    .sort(by=['FACTSET_ENTITY_ID', 'date_q'])
    )


term_.write_parquet(os.path.join(cd, 'short_and_long_term_investors_gaspar2005.parquet'))

print('Short- and long-term investors per Gaspar, Massa & Matos (2005) - END')


# Sanity checks 

# Number of institutions per category through quarters
num_inst = term_.drop('FACTET_ENTITY_ID').group_by('date_q').sum().sort('date_q')

num_inst.to_pandas().set_index('date_q').plot()

# Average churn rate through the years
b = term.group_by('date_q').agg(pl.col('CHURN_RATE_ROLL4Q').mean()).sort(by='date_q')

b.to_pandas().set_index('date_q').plot()



