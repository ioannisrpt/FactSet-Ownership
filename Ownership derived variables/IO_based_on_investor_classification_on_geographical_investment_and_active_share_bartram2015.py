r"""
Classify institutions as global/local and active/passive investors.



I decompose total IO into 6 components:
    1. IO_GA : IO of global and active investors
    2. IO_GP : IO of global and passive 
    3. IO_RA : IO of regional and active 
    4. IO_RP : IO of regional and passive
    5. IO_LA : IO of local and active
    6. IO_LP : IO of local and passive


Market cap is in millions USD.

Input:
   holdingsall_company_level.parquet 
   ...\global_and_local_investors.parquet
   ...\active_share_bartram2015.parquet

    
Output:
    ...\IO_by_geographic_investment_and_active_share_bartram2015.parquet
    ...\IO_AGG_by_geographic_investment_and_active_share_bartram2015.parquet

"""

import os
import polars as pl
import pandas as pd
from functools import reduce
import matplotlib.pyplot as plt


# Current directory
cd = r'C:\Users\FMCC\Desktop\Ioannis'


# Parquet Factset tables
factset_dir =  r'C:\FactSet_Downloadfiles\zips\parquet'



# Create folder to save
save_folder = 'IO_based_on_geographic_investment_and_active_share_bartram2015' 
save_dir = os.path.join(cd, save_folder)
if save_folder not in os.listdir(cd):
    os.mkdir(save_dir)
   


# ~~~~~~~~~~~~
# IMPORT DATA
# ~~~~~~~~~~~~


# Factset market cap holdings at the company level
fh = pl.read_parquet(os.path.join(cd,'holdingsall_company_level.parquet'))



# Global, regional, local investors of Bartram et al. (2015)
investors_type = pl.read_parquet(os.path.join(cd, 'investors_type.parquet')) 


# Active share of Bartram et al. (2015)
active_share = pl.read_parquet(os.path.join(cd, 'active_share_bartram2015.parquet'))


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


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#    GLOBAL, REGIONAL AND LOCAL INVESTORS
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Due to insufficient data, not all institutions are classified to
# global and local investors. Thus I limit my analysis to the classified 
# investors only
investors_type_ = investors_type.filter(pl.col('IS_CLASSIFIED') == 1)


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#     TOTAL INSTITUTIONAL OWNERSHIP IO
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

io = ( 
       fh
       .group_by(['COMPANY_ID', 'date_q'])
       .agg(pl.col('IO').sum())
       .sort(by=['COMPANY_ID', 'date_q'])
       )
#  AGGREGATE MARKET CAP HOLDINGS BY QUARTER
io_sum = ( 
    fh
    .group_by('date_q')
    .agg(pl.col('MKTCAP_HELD').sum().alias('MKTCAP_HELD_ALL'))
    .sort('date_q')
    )

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#   AUGMENT HOLDINGS WITH INVESTOR CLASSIFICATION
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# global, regional and local investors
fh = fh.join(investors_type_, 
             how='left',
             on=['FACTSET_ENTITY_ID', 'date_q'])

# Active and passive investors
fh = fh.join(active_share,
             how='left',
             on=['FACTSET_ENTITY_ID', 'date_q'])


# Keep only observations that are not null
fh_ = fh.drop_nulls()


# Number of firms in the final sample
num_stocks = fh_.select(['COMPANY_ID', 'date_q']).unique()

num_stocks_q = ( 
    num_stocks
    .group_by('date_q')
    .agg(pl.col('COMPANY_ID').count().alias('COUNT'))
    .sort(by='date_q')
    )

num_stocks_q.to_pandas().set_index('date_q').plot()

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#  GLOBAL AND ACTIVE INVESTORS : IO_GA
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Keep only global and active institutions
fh_s = fh_.filter( 
    (pl.col('IS_GLOBAL_INVESTOR') == 1)
    & (pl.col('IS_PASSIVE_INVESTOR') == 0)
    )
#  IO_GA
io_ga = ( 
       fh_s
       .group_by(['COMPANY_ID', 'date_q'])
       .agg(pl.col('IO').sum().alias('IO_GA'))
       .sort(by=['COMPANY_ID', 'date_q'])
       )
#  AGGREGATE MARKET CAP HOLDINGS BY QUARTER
io_ga_sum = ( 
    fh_s
    .group_by('date_q')
    .agg(pl.col('MKTCAP_HELD').sum().alias('MKTCAP_HELD_GA'))
    .sort('date_q')
    )



# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#  GLOBAL AND PASSIVE INVESTORS : IO_GP
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Keep only global and passive institutions
fh_s = fh_.filter( 
    (pl.col('IS_GLOBAL_INVESTOR') == 1)
    & (pl.col('IS_PASSIVE_INVESTOR') == 1)
    )
#  IO_GP
io_gp = ( 
       fh_s
       .group_by(['COMPANY_ID', 'date_q'])
       .agg(pl.col('IO').sum().alias('IO_GP'))
       .sort(by=['COMPANY_ID', 'date_q'])
       )
#  AGGREGATE MARKET CAP HOLDINGS BY QUARTER
io_gp_sum = ( 
    fh_s
    .group_by('date_q')
    .agg(pl.col('MKTCAP_HELD').sum().alias('MKTCAP_HELD_GP'))
    .sort('date_q')
    )


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#  REGIONAL AND ACTIVE INVESTORS : IO_RA
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Keep only regional and active institutions
fh_s = fh_.filter( 
    (pl.col('IS_REGIONAL_INVESTOR') == 1)
    & (pl.col('IS_PASSIVE_INVESTOR') == 0)
    )
#  IO_RA
io_ra = ( 
       fh_s
       .group_by(['COMPANY_ID', 'date_q'])
       .agg(pl.col('IO').sum().alias('IO_RA'))
       .sort(by=['COMPANY_ID', 'date_q'])
       )
#  AGGREGATE MARKET CAP HOLDINGS BY QUARTER
io_ra_sum = ( 
    fh_s
    .group_by('date_q')
    .agg(pl.col('MKTCAP_HELD').sum().alias('MKTCAP_HELD_RA'))
    .sort('date_q')
    )


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#  REGIONAL AND PASSIVE INVESTORS : IO_RP
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Keep only regional and passive institutions
fh_s = fh_.filter( 
    (pl.col('IS_REGIONAL_INVESTOR') == 1)
    & (pl.col('IS_PASSIVE_INVESTOR') == 1)
    )
#  IO_RP
io_rp = ( 
       fh_s
       .group_by(['COMPANY_ID', 'date_q'])
       .agg(pl.col('IO').sum().alias('IO_RP'))
       .sort(by=['COMPANY_ID', 'date_q'])
       )
#  AGGREGATE MARKET CAP HOLDINGS BY QUARTER
io_rp_sum = ( 
    fh_s
    .group_by('date_q')
    .agg(pl.col('MKTCAP_HELD').sum().alias('MKTCAP_HELD_RP'))
    .sort('date_q')
    )

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#  LOCAL AND ACTIVE INVESTORS : IO_LA
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Keep only local and active institutions
fh_s = fh_.filter( 
    (pl.col('IS_LOCAL_INVESTOR') == 1)
    & (pl.col('IS_PASSIVE_INVESTOR') == 0)
    )
#  IO_LA
io_la = ( 
       fh_s
       .group_by(['COMPANY_ID', 'date_q'])
       .agg(pl.col('IO').sum().alias('IO_LA'))
       .sort(by=['COMPANY_ID', 'date_q'])
       )
#  AGGREGATE MARKET CAP HOLDINGS BY QUARTER
io_la_sum = ( 
    fh_s
    .group_by('date_q')
    .agg(pl.col('MKTCAP_HELD').sum().alias('MKTCAP_HELD_LA'))
    .sort('date_q')
    )


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#  LOCAL AND PASSIVE INVESTORS : IO_LP
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Keep only local and passive institutions
fh_s = fh_.filter( 
    (pl.col('IS_LOCAL_INVESTOR') == 1)
    & (pl.col('IS_PASSIVE_INVESTOR') == 1)
    )
# IO_LP
io_lp = ( 
       fh_s
       .group_by(['COMPANY_ID', 'date_q'])
       .agg(pl.col('IO').sum().alias('IO_LP'))
       .sort(by=['COMPANY_ID', 'date_q'])
       )
#  AGGREGATE MARKET CAP HOLDINGS BY QUARTER
io_lp_sum = ( 
    fh_s
    .group_by('date_q')
    .agg(pl.col('MKTCAP_HELD').sum().alias('MKTCAP_HELD_LP'))
    .sort('date_q')
    )


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#    COMPANY LEVEL IO
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Merge
io_comp_ls = [io, io_ga, io_gp, io_ra, io_rp, io_la, io_lp]
io_comp = reduce(lambda a,b : a.join(b, 
                                     how='full',
                                     coalesce=True,
                                     on=['COMPANY_ID','date_q']), io_comp_ls)
# Null values are 0 positions
io_comp = io_comp.fill_null(0)
io_comp = io_comp.sort(['COMPANY_ID', 'date_q'])

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~
#   AGGREGATE LEVEL IO 
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Merge
io_sum_ls = [io_sum, io_ga_sum, io_gp_sum, io_ra_sum, io_rp_sum, io_la_sum, io_lp_sum]
io_agg = reduce(lambda a,b : a.join(b, how='inner', on='date_q'), io_sum_ls)

# To pandas
io_agg_pd = io_agg.to_pandas()
io_agg_pd['date_q'] = pd.to_datetime(io_agg_pd['date_q'], format='%Y%m')
io_agg_pd.set_index('date_q', inplace=True)

# PLOT

plt.figure()
(io_agg_pd/1000000).plot()
plt.xlabel('Quarter')
plt.ylabel('Market capitalization holdings (trillions)')
plt.tight_layout()
plt.savefig(os.path.join(save_dir, 'mcap_held_investors_per_group.png'), 
                dpi = 200, bbox_inches='tight')
plt.close()



# ~~~~~~~~~~~~~~~~~~~~
#   SAVE 
# ~~~~~~~~~~~~~~~~~~


io_comp.write_parquet(os.path.join(cd, 'IO_by_geographic_investment_and_active_share_bartram2015.parquet'))


io_agg.write_parquet(os.path.join(cd, 'IO_AGG_by_geographic_investment_and_active_share_bartram2015.parquet'))





