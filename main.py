"""

    """

import pandas as pd
from githubdata import GithubData
from mirutil.df_utils import print_df_columns_in_dict_fmt as prt


class RepoUrl :
    src = 'https://github.com/imahdimir/raw-d-man-mutual-funds'
    targ = 'https://github.com/imahdimir/d-man-fund-NAV-inflow-outflow'
    cur = 'https://github.com/imahdimir/b-d-funds-NAV-inflow-outflow'

ru = RepoUrl()

class RawDCols :
    row = 'row'
    name = 'name'
    nav = 'nav'
    jd = 'jd'
    jm = 'jm'
    incol = 'in'
    out = 'out'

rd = RawDCols()

class TargCols :
    jm = 'JMonth'
    fund = 'FundName'
    nav = 'NAV'
    inflow = 'Inflow'
    outflow = 'Outflow'

tc = TargCols()

def main() :
    pass

    ##
    rp_src = GithubData(ru.src)
    rp_src.clone()
    ##
    ps = rp_src.data_fp
    dfn = pd.read_excel(ps , sheet_name = 'NAV')
    ##
    dfn = dfn.drop(columns = rd.row)
    ##
    df1 = dfn[[rd.name]].drop_duplicates()

    ptr = 'کل' + r'\b.+'
    msk = df1[rd.name].str.fullmatch(ptr)
    df2 = df1[msk]

    ##
    msk = dfn[rd.name].str.fullmatch(ptr)
    dfn = dfn[~ msk]

    ##
    dfn[tc.jm] = dfn[rd.jd].str[0 :7]

    ##
    dfn = dfn.drop(columns = rd.jd)

    ##
    ren = {
            rd.name : tc.fund ,
            rd.nav  : tc.nav
            }
    dfn = dfn.rename(columns = ren)
    ##
    df1 = dfn[[tc.fund]].drop_duplicates()

    ##
    dff = pd.read_excel(ps , sheet_name = 'Flow')

    ##
    ptr = '\d+'
    msk = ~ dff[rd.row].astype(str).str.fullmatch(ptr)

    msk &= dff[rd.row].notna()

    sr1 = dff.loc[msk , rd.row]
    sr1 = sr1.str.strip()
    sr1 = sr1.drop_duplicates()

    for el in sr1 :
        print('"' + el.strip() + '": None,')

    ##
    not_ok_in_row = {
            "کل ص س در اوراق بهادار با درآمد ثابت(جمع/ میانگین ساده)" : None ,
            "کل ص س مختلط"                                            : None ,
            "صندوقهای سرمایه گذاری در سهام"                           : None ,
            "کل صندوقهای سرمایه گذاری"                                : None ,
            }

    msk = ~ dff[rd.row].astype(str).str.strip().isin(not_ok_in_row.keys())
    dff = dff[msk]

    ##
    msk = ~ dff[rd.name].astype(str).str.strip().isin(not_ok_in_row.keys())
    dff = dff[msk]

    ##
    ptr = '\d+'
    msk = dff[rd.row].astype(str).str.fullmatch(ptr)

    msk |= dff[rd.row].isna()

    assert msk.all()
    ##
    dff = dff.drop(columns = rd.row)

    ##
    ren = {
            rd.name  : tc.fund ,
            rd.jm    : tc.jm ,
            rd.incol : tc.inflow ,
            rd.out   : tc.outflow
            }

    dff = dff.rename(columns = ren)
    ##
    msk = dff[tc.fund].isin(dfn[tc.fund])

    assert msk.all()
    ##
    msk = dfn[tc.fund].isin(dff[tc.fund])

    msk.all()
    ##
    dfn1 = dfn.merge(dff , on = [tc.fund , tc.jm] , how = 'outer')

    ##
    msk = dfn1.isna().any(axis = 1)

    df1 = dfn1[msk]
    ##
    msk = dfn1[tc.nav].eq(0)
    df1 = dfn1[msk]

    dfn1.loc[msk , tc.nav] = None
    ##
    dfn1 = dfn1[[tc.jm , tc.fund , tc.nav , tc.inflow , tc.outflow]]

    ##
    rp_targ = GithubData(ru.targ)
    rp_targ.clone()

    ##
    fp = rp_targ.local_path / 'data.xlsx'
    dfn1.to_excel(fp , index = False)

    ##
    msg = 'builded by: '
    msg += ru.cur
    ##
    rp_targ.commit_and_push(msg)

    ##

    ##


    ##

    ##

##
if __name__ == "__main__" :
    main()

##
# noinspection PyUnreachableCode
if False :

    pass

    ##
    dfn1['NetInflow'] = dfn1[tc.inflow] - dfn1[tc.outflow]

    ##
    dfn1['NAV_{t+1}'] = dfn1.groupby(tc.fund)[tc.nav].shift(-1)

    ##
    import requests


    ##
    dfn1.to_excel('temp.xlsx' , index = False)
    ##
    df = pd.read_excel('temp.xlsx')
    ##
    url = 'http://www.tsetmc.com/Loader.aspx?ParTree=151324&i=11197'

    r = requests.get(
        url , verify = False , headers = {
                'User-Agent' : 'Mozilla/5.0'
                }
        )
    with open('resp.txt' , 'w') as fi :
        fi.write(r.text)

    ##
    df1 = pd.read_excel('temp1.xlsx')

    ##
    df1 = df1.drop(columns = 'SEORegisterNo')

    ##
    df1 = df1[['Name' , 'JMonth' , 'navStat']]
    ##
    df = df.merge(
        df1 ,
        left_on = ['FundName' , 'JMonth'] ,
        right_on = ['Name' , 'JMonth'] ,
        how = 'left'
        )

    ##
    prt(df)

    ##
    cwk = {
            "FundName"  : None ,
            "JMonth"    : None ,
            "NAV"       : None ,
            "Inflow"    : None ,
            "Outflow"   : None ,
            "NetInflow" : None ,
            "navStat"   : None ,
            }
    df = df[cwk.keys()]

    ##
    ren = {
            'NAV'     : 'NetAsset' ,
            'navStat' : 'NAV' ,
            }

    df = df.rename(columns = ren)
    ##
    co = {
            "FundName"  : None ,
            "JMonth"    : None ,
            "NetAsset"  : None ,
            "NAV"       : None ,
            "Inflow"    : None ,
            "Outflow"   : None ,
            "NetInflow" : None ,
            }

    df = df[co.keys()]

    ##
    df['NAV_{t+1}'] = df.groupby(tc.fund)[tc.nav].shift(-1)

    ##
    df['gNav'] = df.groupby(tc.fund)[tc.nav].pct_change()

    ##
    df.to_excel('funds.xlsx' , index = False)

    ##
    from mirutil.df_utils import save_df_as_a_nice_xl as sxl


    ##
    sxl(df , 'funds.xlsx')  ##

##

##
