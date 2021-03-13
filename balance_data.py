import pandas as pd
import numpy as np
pd.options.display.max_columns = None

def add_metrics(df):
    df.drop_duplicates(inplace=True)
    # gross profit
    df.replace('-', np.nan, inplace=True)
    df.totaloperatereve = df.totaloperatereve.astype(float)
    df['gross_profit_r'] = 1 - df.operateexp.astype(float) / df.totaloperatereve

    # net profit
    df['net_profit_r'] = 1 - df.totaloperateexp.astype(float) / df.totaloperatereve

    # 扣非, exclude 1 na
    df['koufei_profit_r'] = df.kcfjcxsyjlr.astype(float) / df.totaloperatereve


def get_increase_rate(df, y1, y2, cols):
    # assume dfs contains all consecutive year data between y1 and y2
    r = df[['sname'+str(y2)]].rename(columns={'sname'+str(y2):'sname'})
    for i in range(y1+1, y2+1):
        for col in cols:
            r.loc[:, col+str(i)] = df[col+str(i)].astype(float) / df[col+str(i-1)].astype(float)
    return r


y1 = 2010
y2 = 2019
dfs = [pd.read_csv(f'eastmoney/income{year}.csv') for year in range(y1, y2+1)]
for df in dfs: add_metrics(df)
for i, df in enumerate(dfs): df.rename(columns={x:x+str(i+2010) for x in df.columns if x != 'scode'}, inplace=True)

df = dfs[-1]
for _df in dfs[:-1]:
    df = df.merge(_df, on='scode', how='left')

cols = ['totaloperatereve', 'kcfjcxsyjlr', 'gross_profit_r']
r = get_increase_rate(df, y1, y2, cols)
check = r[[x for x in r.columns if x!='sname']]
check = (check > 1.1) | (check.isna())
r.loc[check[check.all(axis=1)].index, 'sname']


