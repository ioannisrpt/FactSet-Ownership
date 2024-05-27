# -*- coding: utf-8 -*-
"""
Compare with Diego's  calculations
"""


import os
import polars as pl


# ~~~~~~~~~~~~~~~~~~
#    DIRECTORIES 
# ~~~~~~~~~~~~~~~~~~


factset_dir = r'C:\Users\ropot\Desktop\Financial Data for Research\FactSet'

ddir = r'C:\Users\ropot\Desktop\Financial Data for Research\FactSet\Factset_ownership_raw_parts'


# ~~~~~~~~~~~~
#  IMPORT DATA
# ~~~~~~~~~~~~~


fh = pl.read_parquet(os.path.join(factset_dir, 'factset_mcap_holdings.parquet'),
                              use_pyarrow=True)

"""
fh = pl.read_parquet(os.path.join(factset_dir, 'scheme_2_mcap_held.parquet'),
                              use_pyarrow=True)
"""



#   COMPARING WITH DIEGO'S CALCULATIONS


quarter_date = 200103

# diegos replication
d = pl.read_csv(os.path.join(ddir, 'own_raw_%d.csv' % quarter_date))
d = d.cast({'date_q': pl.Int32})

# ioannis for 199903
i = fh.filter(pl.col('date_q') == quarter_date)
i = i.rename({'FACTSET_ENTITY_ID' : 'entity_id',
                  'FSYM_ID' : 'fsym_id',
                  'MCAP_HELD' : 'hcap_i'})

# Merge
m = d.join(i, how='inner', on=['fsym_id', 'entity_id', 'date_q'])

m = m.with_columns(
    (pl.col('hcap') - pl.col('hcap_i')).alias('diff')
    )

m_ = m.to_pandas()

print(m_[['hcap', 'hcap_i']].corr())

m_['abs_diff'].describe()


print(m_.loc[m_['SCHEME'] == 4, ['hcap', 'hcap_i']].corr())