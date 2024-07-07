# -*- coding: utf-8 -*-
"""
Replication of Ferreira & Matos (2008) methodology

PART 2 - MUTUAL FUNDS REPORTS + IMPUTATION

Market cap is in millions of USD.

Input:
    
    
Output:
    v2_holdingsmf.parquet
    hmktcap.parquet
    
"""


import os
import polars as pl


# ~~~~~~~~~~~~~~~~~~
#    DIRECTORIES 
# ~~~~~~~~~~~~~~~~~~

# Current directory
cd = r'C:\Users\FMCC\Desktop\Ioannis'

# Parquet Factset tables
factset_dir =  r'C:\FactSet_Downloadfiles\zips\parquet'

# 13F filings
own_inst_13f_dir = os.path.join(factset_dir, 'own_inst_eq_v5_full')

# Stakes based sources including UKSR, RNS, 13D/G's, proxies, etc.
own_stakes_dir = os.path.join(factset_dir, 'own_stakes_eq_v5_full')

# Mutual fund holdings
funds_dir = os.path.join(factset_dir, 'own_fund_eq_v5_full_split_by_fund')




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

# Import sym_coverage table
sym_cov = pl.read_parquet(os.path.join(factset_dir, 'sym_coverage.parquet'),
                              use_pyarrow=True,
                              columns=['FSYM_ID', 'FREF_SECURITY_TYPE'])

# Import own_ent_funds table 
own_ent_funds = pl.read_parquet(os.path.join(factset_dir, 'own_ent_funds.parquet'),
                                use_pyarrow=True)
# the table is used to match a Fund to the Institution that manages it
own_ent_funds = ( 
            own_ent_funds
            .select(['FACTSET_FUND_ID', 'FACTSET_INST_ENTITY_ID'])
            .rename({'FACTSET_INST_ENTITY_ID': 'FACTSET_ENTITY_ID'})
            )

# Import own_sec_entity_eq table
own_sec_entity_eq = pl.read_parquet(os.path.join(factset_dir, 'own_sec_entity_eq.parquet'),
                                use_pyarrow=True)


# Prices
own_sec_prices = pl.read_parquet(os.path.join(factset_dir, 'own_sec_prices_eq.parquet'),
                                 use_pyarrow=True)



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



# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#     SOME HOUSKEEPING
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


# termination_date TABLE (Termination quarter for each owneship security)
termination_date = ( 
    own_sec_prices
    .group_by('FSYM_ID')
    .agg(pl.col('date_q').max().alias('TERMINATION_DATE'))
    )



# Keep necessary columns
own_sec_cov_ = own_sec_cov.select(['FSYM_ID',
                                   'ISSUE_TYPE',
                                   'ISO_COUNTRY'])

# Join
equity_secs = own_sec_cov_.join(sym_cov, how='left', on=['FSYM_ID'])

# Equity or ADR
filter1 = pl.col('ISSUE_TYPE').is_in(['EQ', 'AD'])

# Preffered share
filter2 = pl.col('ISSUE_TYPE').is_in(['PF']) & pl.col('FREF_SECURITY_TYPE').is_in(['PREFEQ'])

# equity_secs TABLE (Universe of stocks)
equity_secs = equity_secs.filter(filter1 | filter2)

# own_basic TABLE (Universe of stocks plus information)
own_basic = ( 
    equity_secs
    .join(own_sec_entity_eq, how='inner', on=['FSYM_ID'])
    .join(termination_date, how='inner', on=['FSYM_ID'])
    )

# prices_historical TABLE 
prices_historical = own_sec_prices_q

# Free memory
del own_sec_cov, own_sec_cov_, equity_secs, termination_date, own_sec_prices

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~
#    OWN MARKET CAP PROCEDURE
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~

#  own_mv TABLE (market cap at the security level)
own_mv = ( 
    own_sec_prices_q.with_columns(
    (pl.col('ADJ_PRICE') * pl.col('ADJ_SHARES_OUTSTANDING')).alias('ADJ_OWN_MV'),
    (pl.col('UNADJ_PRICE') * pl.col('UNADJ_SHARES_OUTSTANDING')).alias('UNADJ_OWN_MV')
    )
    .select(['FSYM_ID', 'date_q', 'ADJ_OWN_MV', 'UNADJ_OWN_MV'])
    )

# Use information of unadjusted prices if information from adjusted prices is 
# insufficient.
own_mv = own_mv.with_columns(
    pl.when(pl.col('ADJ_OWN_MV') == 0)
    .then(pl.col('UNADJ_OWN_MV'))
    .otherwise(pl.col('ADJ_OWN_MV'))
    .alias('OWN_MV')
    )

# Market cap at the security level for non-ADR universe of stocks
own_mktcap1 = ( 
    own_mv.select(['FSYM_ID', 'date_q', 'OWN_MV'])
    .join(own_basic.select(['FSYM_ID', 'FACTSET_ENTITY_ID', 'ISSUE_TYPE', 'FREF_SECURITY_TYPE']), 
          how='inner', on=['FSYM_ID'])
    .filter(pl.col('ISSUE_TYPE') != 'AD')
    )


r"""
* unilever;
proc sql;
delete from own_mktcap1 where fsym_id eq 'DXVFL5-S' and price_date ge '30SEP2015'd;
"""


# own_mktcap TABLE (market cap at the firm level
own_mktcap = (
    own_mktcap1
    .group_by(['FACTSET_ENTITY_ID', 'date_q'])
    .agg(pl.col('OWN_MV').sum())
    )


# hmktcap TABLE (market cap at the firm-level + housekeeping)
hmktcap = ( 
    own_mktcap.with_columns(
    (pl.col('OWN_MV')/1000000).alias('MKTCAP_USD')
    )
    .select(['FACTSET_ENTITY_ID', 'date_q', 'MKTCAP_USD'])
    )
hmktcap = hmktcap.filter(pl.col('MKTCAP_USD').is_not_null() &
                         (pl.col('MKTCAP_USD') > 0))
hmktcap = hmktcap.sort(by=['FACTSET_ENTITY_ID', 'date_q'])

# Save 
hmktcap.write_parquet(os.path.join(cd, 'hmktcap.parquet'))
                     

# Free memory
del own_mv, own_mktcap, own_mktcap1

# ///////////////////////////////////////////////////////

#        MUTUAL FUNDS REPORTS 

# ///////////////////////////////////////////////////////

print('Mutual funds reports \n')


# Mutual funds reports tables are so big that cannot be handled all
# at once at my machine as in Ferreira & Matos (2008). To ease the 
# computational burden of the mutual funds calculation I do the following:
# i)   Break the mutual funds dataset into 17 tables in chunks of 10,000 funds 
# ii)  Filter securities as per Ferreira & Matos (2008)
# iii) Drop any US securities from the tables

# Output table after iteration through funds datasets
output_table = pl.DataFrame()

dataset = 'funds_table_1.parquet'

# Iterate through the mutual funds datasets
for dataset in os.listdir(funds_dir):
    
    print('%s is processed \n' % dataset)
    
    
    # Import mutual funds dataset
    own_fund = pl.read_parquet(os.path.join(funds_dir, dataset),
                               use_pyarrow=True,
                               columns = ['FACTSET_FUND_ID',
                                          'FSYM_ID',
                                          'REPORT_DATE',
                                          'ADJ_HOLDING'])
    
    # Define quarter 'date_q' in integer format based on 'REPORT_DATE'
    own_fund = apply_quarter_scheme(own_fund, 'REPORT_DATE')
    
    # Sort 
    own_fund = (own_fund.sort(by=['FACTSET_FUND_ID', 
                                  'FSYM_ID', 
                                  'REPORT_DATE']))
    
    # auxmf TABLE (13F reports with the most recent report date within quarter)
    auxmf = ( 
        own_fund
        .group_by(['FACTSET_FUND_ID','FSYM_ID', 'date_q'])
        .agg(pl.all().sort_by('REPORT_DATE').last())
        .sort(by=['FACTSET_FUND_ID','FSYM_ID', 'date_q'])
        )
    
    # v1_holdingsmf TABLE (Mutual funds reported positions for universe of stocks
    # plus company level market capitalization)
    
    # Market capitalizaton at the firm level for securities
    hmktcap_ = hmktcap.join(own_basic.select(['FSYM_ID', 'FACTSET_ENTITY_ID', 'ISO_COUNTRY']),
                            how='inner', 
                            on=['FACTSET_ENTITY_ID'])
    hmktcap_ = hmktcap_.rename({'FACTSET_ENTITY_ID' : 'COMPANY_ID'})
    
    # Augment with adjusted prices at the security level
    hmktcap_prc = hmktcap_.join(prices_historical.select(['FSYM_ID', 'date_q', 'ADJ_PRICE']),
                             how='inner',
                             on=['FSYM_ID', 'date_q'])
    
    
    v1_holdingsmf = auxmf.join(hmktcap_prc, 
                                 how='inner',
                                 on=['FSYM_ID', 'date_q'])
    v1_holdingsmf = ( 
        v1_holdingsmf.with_columns(
        (pl.col('ADJ_HOLDING')*pl.col('ADJ_PRICE')/1000000).alias('MKTCAP_HOLDING')
        )
        .drop(['ADJ_PRICE'])
        )
    v1_holdingsmf = v1_holdingsmf.with_columns(
        (pl.col('MKTCAP_HOLDING')/pl.col('MKTCAP_USD')).alias('IO')
        )
    v1_holdingsmf = ( 
                    v1_holdingsmf.select(['FACTSET_FUND_ID',
                                            'FSYM_ID',
                                            'date_q',
                                            'ADJ_HOLDING',
                                            'MKTCAP_HOLDING',
                                            'MKTCAP_USD',
                                            'IO',
                                            'COMPANY_ID',
                                            'ISO_COUNTRY',
                                            'REPORT_DATE'])
                    .sort(by=['FACTSET_FUND_ID',
                              'FSYM_ID',
                              'date_q'])
                    )


    # Free memory
    del auxmf, hmktcap_, hmktcap_prc    
    
    
# ///////////////////////////////////////////////////////

#        MUTUAL FUNDS REPORTS IMPUTATION

# ///////////////////////////////////////////////////////

    print('Mutual funds reports - Imputation \n')
    
    
    # sym_range TABLE (Find the termination quarter for each security)
    sym_range = ( 
        own_basic
        .select(['FSYM_ID', 'TERMINATION_DATE'])
        .rename({'TERMINATION_DATE' : 'maxofqtr'})
        .unique()
        )
    
    # rangeofquarters TABLE (all quarters for which FactSet has data)
    rangeofquarters = v1_holdingsmf.select(['date_q']).unique().sort('date_q')
    
    # insts_mf TABLE (all funds for which FactSet has data)
    insts_mf = ( 
        v1_holdingsmf
        .select(['FACTSET_FUND_ID'])
        .unique()
        .sort('FACTSET_FUND_ID')
        )
    
    # insts_mfdates TABLE (all possible fund-quarter pairs)
    insts_mfdates = insts_mf.join(rangeofquarters, how='cross')
    
    # pairs_mf TABLE (all fund-quarter pairs)
    pairs_mf = ( 
        v1_holdingsmf
        .select(['FACTSET_FUND_ID', 'date_q'])
        .unique()
        .sort(['FACTSET_FUND_ID', 'date_q'])
        )
    pairs_mf = pairs_mf.with_columns(
        pl.lit(1).alias('HAS_REPORT')
        )
    
    # fund_minmax TABLE (mininum and maximum quarter that each funds reports)
    fund_minmax = ( 
        v1_holdingsmf
        .group_by(['FACTSET_FUND_ID'])
        .agg(
            pl.col('date_q').min().alias('min_quarter'),
            pl.col('date_q').max().alias('max_quarter')
            )
        )    
        
    
    # roll1mf TABLE (master dataset that tracks reporting dates for mutual funds)
    roll1mf = insts_mfdates.join(fund_minmax,
                                   how='left', 
                                   on=['FACTSET_FUND_ID'])
    # Kepp fund-quarter pairs that fall within the minimum and maximum 
    # reporting mutual fund quarter
    roll1mf = roll1mf.filter(
        (pl.col('min_quarter') <= pl.col('date_q')) & 
        (pl.col('date_q') <= pl.col('max_quarter'))
        )
    
    # Augment with valid fund-quarter pairs 
    roll1mf = roll1mf.join(pairs_mf, 
                             how='left', 
                             on=['FACTSET_FUND_ID', 'date_q'])
    
    roll1mf = roll1mf.with_columns(
        pl.when(pl.col('HAS_REPORT').is_null())
        .then(0)
        .otherwise(pl.col('HAS_REPORT'))
        .alias('HAS_REPORT')
        )
    
    
    # Create a dummy variable that is 1 if the last report of the fund
    # is within 7 quarters from the current quarter.
    roll1mf = roll1mf.with_columns(
        pl.when(pl.col('HAS_REPORT')==1)
        .then(pl.col('date_q'))
        .otherwise(None)
        .alias('REPORT_QUARTER')
        )
    # Forward fill for 7 quarters the REPORT_QUARTER
    roll1mf = roll1mf.with_columns(
        pl.col('REPORT_QUARTER')
        .forward_fill(limit=7)
        .over(['FACTSET_FUND_ID'])
        .alias('LAST_REPORT_QUARTER')
        )
    # Define the differce in quarters between the current quarter and the last
    # report quarter
    roll1mf = roll1mf.with_columns(
        (
        ((pl.col('date_q')/100).floor() -
        (pl.col('LAST_REPORT_QUARTER')/100).floor())*4 +
        ((pl.col('date_q').mod(100)) -
        (pl.col('LAST_REPORT_QUARTER').mod(100)))/3
        )
        .cast(pl.Int32)
        .alias('DIFF_QUARTERS')
        )
    
    # If the difference between current and last reported quarter is 
    # less or equal than 7, then VALID=1, otherwise 0
    roll1mf = roll1mf.with_columns(
        pl.when(pl.col('DIFF_QUARTERS')<=7)
        .then(1)
        .otherwise(0)
        .alias('VALID')
        )
    
    # Example for sanity check
    roll1mf.filter(pl.col('FACTSET_FUND_ID') == '04B8D4-E').write_csv(os.path.join(cd, 'mf_example.csv'))
    
    
    # Fill_mf TABLE (fund-quarter pairs to be filled by previous reports)
    fill_mf = (
        roll1mf
        .filter((pl.col('HAS_REPORT')==0) & (pl.col('VALID')==1))
        .select(['FACTSET_FUND_ID', 'date_q', 'LAST_REPORT_QUARTER'])
        .unique(['FACTSET_FUND_ID', 'date_q'])
        )


    # inserts_mf TABLE (Fill the quarterly reports of the mutual funds
    # that are missing and are within the 7 quarter mark of the last reported
    # quarter)
    v1_holdingsmf_ = ( 
                        v1_holdingsmf
                        .select(['FACTSET_FUND_ID',
                                'FSYM_ID',
                                'date_q',
                                'IO',
                                'COMPANY_ID',
                                'ISO_COUNTRY'])
                        .rename({'date_q':'LAST_REPORT_QUARTER'})
                        )
    
    inserts_mf = fill_mf.join(v1_holdingsmf_,
                                how='left',
                                on=['FACTSET_FUND_ID', 'LAST_REPORT_QUARTER'])
    
    # Account for the termination date of each security 'FSYM_ID'
    inserts_mf = inserts_mf.join(sym_range, on=['FSYM_ID'])
    # Drop security-quarter pairs for which the quarter exceeds the termination
    # date
    inserts_mf = inserts_mf.filter(pl.col('date_q')<=pl.col('maxofqtr'))
    
    # Some housekeeping 
    inserts_mf = (
        inserts_mf
        .drop(['maxofqtr', 'LAST_REPORT_QUARTER'])
        .sort(by=['FACTSET_FUND_ID', 'FSYM_ID', 'date_q'])
        )
    
    
    # Free memory
    del sym_range, rangeofquarters, insts_mf, pairs_mf, roll1mf, fill_mf, v1_holdingsmf_ 
       
        
    # The implicit assumption of the imputation method is that funds 
    # that are missing a report in intermediate quarters hold the same stocks
    # as the last valid reported quarter in the same percentage IO. Thus
    # I can back out the market cap holdings.
    # I cannot back out the adjusted holdings.
    inserts_mf = inserts_mf.join(hmktcap.rename({'FACTSET_ENTITY_ID': 'COMPANY_ID'}),
                                   how='inner',
                                   on=['COMPANY_ID', 'date_q'])
    inserts_mf = inserts_mf.with_columns(
        (pl.col('IO')*pl.col('MKTCAP_USD')).alias('MKTCAP_HOLDING')
        ) 
    
    
    # v2_holdingsmf TABLE (Raw and imputated fund-security-quarter pairs)
    inserts_mf = inserts_mf.with_columns(
        pl.lit(None).alias('ADJ_HOLDING'),
        pl.lit(None).alias('REPORT_DATE')
        )
    inserts_mf = inserts_mf.select(v1_holdingsmf.columns)
    v1_holdingsmf_ = pl.concat([v1_holdingsmf, inserts_mf])
    
    # Sort and keep unique fund-security-quarter pairs
    v1_holdingsmf_ = (
        v1_holdingsmf_
        .sort(by=['FACTSET_FUND_ID', 'FSYM_ID', 'date_q'])
        .unique(['FACTSET_FUND_ID', 'FSYM_ID', 'date_q'])
        )
    
    
    # Free memory
    del v1_holdingsmf, inserts_mf
    
    # Assign each fund to the institution that manages it
    v1_holdingsmf_ = v1_holdingsmf_.join(own_ent_funds,
                                       on=['FACTSET_FUND_ID'])
    


    # Concat
    output_table = pl.concat([output_table, v1_holdingsmf_])
    
    # Free memory
    del v1_holdingsmf_
    


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#     AGGREGATE OVER INSTITUTIONS 
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

print('Aggregate over institutions \n')

# Aggregate over institutions managing the funds -- INSANE COMPUTATION
v2_holdingsmf = (
    output_table
    .group_by(['FACTSET_ENTITY_ID', 'FSYM_ID', 'date_q'])
    .agg(pl.col('MKTCAP_HOLDING').sum(),
         pl.col('IO').sum())
    .sort(by=['FACTSET_ENTITY_ID', 'FSYM_ID', 'date_q'])
    )

# Augment with all other information 
other_info = (
    output_table
    .select(['FSYM_ID', 'date_q', 'MKTCAP_USD', 'COMPANY_ID', 'ISO_COUNTRY'])
    .unique()
    )

# Free memory
del output_table

v2_holdingsmf = v2_holdingsmf.join(other_info, 
                                   how='left',
                                   on=['FSYM_ID', 'date_q'])

# ~~~~~~~~~~~~~
#     SAVE
# ~~~~~~~~~~~~

v2_holdingsmf.write_parquet(os.path.join(cd, 'v2_holdingsmf.parquet'))






