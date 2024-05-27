r"""
Who is an informed investor?


I use the definition of the average churn rate of Yan and Zhang (2009)
to classify institutions into short- and long-term investors. The short-term
investors are better informed and as such they trade and move prices worldwide.

The nominator in churn rate is the minimum of aggregate purchases and sales
for each institution-quarter pair.

Yan, X., & Zhang, Z. (2009). Institutional investors and equity returns: 
    are short-term institutions better informed?.
    The Review of Financial Studies, 22(2), 893-924.

Input:
   \Factset\factset_ownership_holdings_prices_equityShares.parquet 

    
Output:
    ...\investors_short_long_term_yan2009.parquet

"""

import os
import polars as pl


wdir = r'C:\Users\ropot\Desktop\Python Scripts\Role of institutional investors on integration'
os.chdir(wdir)

# Set up environment
import env

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


# Factset ownership holdings
fh = pl.read_parquet(env.factset_dir('factset_ownership_holdings_prices_equityShares.parquet'))

# Own sec prices
own_sec_prices = pl.read_parquet(env.factset_dir('own_sec_prices_eq.parquet'))


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#  KEEP THE MOST RECENT PRICE DATA WITHIN QUARTER
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Define quarter date 'date_q'
own_sec_prices = apply_quarter_scheme(own_sec_prices, 'PRICE_DATE')


# Keep only the most recent 'price' observation within a quarter
# for each security
prices_q = ( 
    own_sec_prices
    .group_by(['FSYM_ID', 'date_q'])
    .agg([pl.all().sort_by('PRICE_DATE').last()])
    )


# Select only relevant columns
prices_q = prices_q.select(['FSYM_ID', 
                          'date_q', 
                          'ADJ_PRICE'])

# Sort
prices_q = prices_q.sort(by=['FSYM_ID', 'date_q'])


#  Define price of previous quarter
prices_q = prices_q.with_columns(
    pl.col('ADJ_PRICE').shift().over('FSYM_ID').alias('ADJ_PRICE_LAG1')
    )


# Free memory 
del own_sec_prices





# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#   MIN AND MAX DATES FOR EACH SECURITY-INSTITUTION PAIR
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Find the min and max quarter for each institution-security
min_max_q = (
    fh
    .group_by(['FACTSET_ENTITY_ID', 'FSYM_ID'])
    .agg( 
    pl.col('date_q').min().alias('date_q_min'),
    pl.col('date_q').max().alias('date_q_max')
    )
    )


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# KEEP PRICE DATA AND INTERMEDIATE QUARTER DATES FOR INSTITUTION-SECURITY PAIR
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


min_max_q_ = min_max_q.join(prices_q, how='left', on=['FSYM_ID'])

min_max_q_ = min_max_q_.filter(
    (pl.col('date_q_min') <= pl.col('date_q') )
    & (pl.col('date_q') <= pl.col('date_q_max') )
    )

min_max_q_ = min_max_q_.select(['FACTSET_ENTITY_ID',
                                'FSYM_ID',
                                'date_q',
                                'ADJ_PRICE',
                                'ADJ_PRICE_LAG1'])

# Free memory
del min_max_q, prices_q


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#   AUGMENT HOLDINGS WITH PRICE DATA AND INTERMEDIATE QUARTER DATES
# ~~~~~~~~~~~~~~~~~~~~~~~~~~


# I LEFT merge the factset holdings because I want to 
# augment it with the full history of intermediate quarter dates of 
# securities that institutions hold. That will allow me to calculate 
# aggregate purchases and sales using simply a lag operator.
fh_prices = min_max_q_.join(fh.drop('ADJ_PRICE'), 
                           how='left', 
                           on=['FACTSET_ENTITY_ID', 'FSYM_ID', 'date_q'])


# Lag holdings by one quarter
fh_prices = fh_prices.with_columns(
    pl.col('ADJ_SHARES_HELD')
    .shift()
    .over(['FACTSET_ENTITY_ID', 'FSYM_ID'])
    .alias('ADJ_SHARES_HELD_LAG1')
    )

# Fill with 0 the current and lagged holdings of institution-security pairs 
# in intermediate quarter dates when there is no holdings value
fh_prices = fh_prices.with_columns(
    pl.col('ADJ_SHARES_HELD').fill_null(0),
    pl.col('ADJ_SHARES_HELD_LAG1').fill_null(0)
    )



# Keep only the necessary columns for the calculation of churn rate
fh_prices_ = fh_prices.select(['FACTSET_ENTITY_ID',
                               'FSYM_ID',
                               'date_q',
                               'ADJ_SHARES_HELD',
                               'ADJ_SHARES_HELD_LAG1',
                               'ADJ_PRICE',
                               'ADJ_PRICE_LAG1'])


#  CHANGE IN HOLDINGS FROM QUARTER TO QUARTER
fh_prices_ = fh_prices_.with_columns(
    (pl.col('ADJ_SHARES_HELD') - pl.col('ADJ_SHARES_HELD_LAG1'))
    .alias('DeltaS')
    )

# DEFINE BUY OR SELL 
fh_prices_ = fh_prices_.with_columns(
    pl.when(pl.col('DeltaS')>0)
    .then(1)
    .otherwise(0)
    .alias('IS_BUY')
    )


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#   AGGREGATE PURCHASE FOR INSTITUTION-QUARTER
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Keep only the buys 
fh_prices_buy = fh_prices_.filter(pl.col('IS_BUY') == 1)

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
fh_prices_sell = fh_prices_.filter(pl.col('IS_BUY') == 0)

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

# Holdings market cap
hcap = fh_prices_.with_columns(
    (pl.col('ADJ_SHARES_HELD')*pl.col('ADJ_PRICE')).alias('HCAP'),
    (pl.col('ADJ_SHARES_HELD_LAG1')*pl.col('ADJ_PRICE_LAG1')).alias('HCAP_LAG1')    
    )

# average of holdings market cap
hcap = hcap.with_columns(
    ( 0.5*(pl.col('HCAP') + pl.col('HCAP_LAG1')) ).alias('HCAP_AVG')
    )

# Select cols
hcap = hcap.select(['FACTSET_ENTITY_ID', 'date_q', 'HCAP_AVG'])

# average share position as denominator
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



# Use 'denom' dataset as reference
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


# minimum value of aggregate purchase or sale
churn = churn.with_columns(
    pl.when(pl.col('PURCHASE')>pl.col('SALE'))
    .then(pl.col('SALE'))
    .otherwise(pl.col('PURCHASE'))
    .alias('MIN_AGG')
    )

# Churn rate
churn = churn.with_columns(
    (pl.col('MIN_AGG') / pl.col('AVERAGE_POSITION'))
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


# ~~~~~~~~~~~~~~~~~~~~~~
#      SAVE 
# ~~~~~~~~~~~~~~~~~~

term_ = ( 
    term.select(['FACTSET_ENTITY_ID', 
                   'date_q',
                   'IS_SHORT_TERM_INVESTOR',
                   'IS_LONG_TERM_INVESTOR'])
    .sort(by=['FACTSET_ENTITY_ID', 'date_q'])
    )


term_.write_parquet(env.investors_integration_dir('investors_short_long_term_yan2009.parquet'))







