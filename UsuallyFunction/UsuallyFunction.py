from datetime import timedelta
from unicodedata import decimal
from pandas import DataFrame
from decimal import Decimal, Context
from bs4 import BeautifulSoup as bs

def date_range(start, stop, step = timedelta(1)):
    current = start
    while current < stop:
        yield current
        current += step

def get_sector_code_df():
    
    df = DataFrame({
        'name'  :['水産・農林業','鉱業','建設業','食料品','繊維製品','パルプ・紙','化学','医薬品','石油・石炭','ゴム製品','ガラス・土石','鉄鋼','非鉄金属','金属製品','機械','電気機器','輸送用機器','精密機器','その他製品','電気・ガス','陸運業','海運業','空運業','倉庫・運輸','情報・通信業','卸売業','小売業','銀行業','証券・商品','保険業','その他金融業','不動産業','サービス業'],
        'id'  :list(range(15,48)),
        'code' :['0050','1050','2050','3050','3100','3150','3200','3250','3300','3350','3400','3450','3500','3550','3600','3650','3700','3750','3800','4050','5050','5100','5150','5200','5250','6050','6100','7050','7100','7150','7200','8050','9050']
    })
    
    return df
    
def get_sector_code(sector:str):
    
    df = get_sector_code_df()
    df = DataFrame(df)
    for row in df.itertuples():
        if row.name == sector:
            return row.code
    return None

def decimal_normalize(f:Decimal):
    if not isinstance(f, str):
        def _remuve_exponent(d:Decimal):
            return d.quantize(Decimal(1)) if d == d.to_integral() else d.normalize()
        a = Decimal.normalize(Decimal(str(f)))
        b = _remuve_exponent(a)
        c = '{:,}'.format(b)
        return str(c)
    else:
        return '-'
