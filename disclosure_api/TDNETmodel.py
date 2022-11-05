from decimal import Decimal
from unicodedata import decimal
import pandas as pd
from bs4 import BeautifulSoup
from os import path

path_taxonomy_labels = './taxonomy_tdnet_label.tsv'

# 財務諸表の要素タグからDFを返す関数。
# 5.のサンプルコードからXBRLファイルの読み込みを省略し、引数をタグに変更。
def get_df(arg_tags):
    # 各fsの各要素を格納した辞書を入れるカラのリスト作成
    list_fs = []
    # 各fsの各要素を辞書に格納
    for each_item in arg_tags:
        dict_fs = {}
        dict_fs['account_item'] = each_item.get('name')
        dict_fs['contextRef'] = each_item.get('contextref')
        dict_fs['format'] = each_item.get('format')
        dict_fs['decimals'] = each_item.get('decimals')
        dict_fs['scale'] = each_item.get('scale')
        dict_fs['unitRef'] = each_item.get('unitRef')
        # マイナス表記の場合の処理＋円単位への変更
        if each_item.get('sign') == '-' and each_item.get('xsi:nil') != 'true':
            try:
                amount = int(each_item.text.replace(',', '')) * -1 * 10 ** int(each_item.get('scale'))
            except ValueError:
                amount = Decimal(each_item.text.replace(',', '')) * -1 * 10 ** Decimal(each_item.get('scale'))
        elif each_item.get('xsi:nil') != 'true':
            try:
                amount = int(each_item.text.replace(',', '')) * 10 ** int(each_item.get('scale'))
            except:
                amount = Decimal(each_item.text.replace(',', '')) * 10 ** Decimal(each_item.get('scale'))
        else:
            amount = ''
        dict_fs['amount'] = amount
        # 辞書をリストへ格納
        list_fs.append(dict_fs)

    # 辞書を格納したリストをDFに
    df_eachfs = pd.DataFrame(list_fs)

    return df_eachfs


# XBRLのパスから、DFを返す関数本体。
def get_fs(arg_path):
    # fsファイルの読み込み。bs4でパース
    with open(arg_path, encoding='utf-8') as f:
        soup = BeautifulSoup(f.read(), 'lxml')

    # nonNumericタグのみ抽出
    tags_nonnumeric = soup.find_all('ix:nonnumeric')

    # 財務諸表要素は'ix:nonFraction'に入っているため、このタグを取得
    tag_nonfraction = soup.find_all('ix:nonfraction')
    # 財務諸表の各要素をDFに。財務諸表区分とタグを引数にして関数に渡す。
    df_each_fs = get_df(tag_nonfraction)
    
     # 並べ替え
    df_each_fs = df_each_fs[['account_item', 'contextRef', 'format', 'decimals', 'scale', 'unitRef', 'amount']]

    return df_each_fs

def get_labeled_df(arg_fs):
    
    # 標準ラベルの読み込み
    df_label_global = pd.read_table(path_taxonomy_labels, sep='\t', encoding='utf-8')
    # 標準ラベルの処理
    
    # 標準ラベルデータのうち、必要行に絞る
    df_label_global = df_label_global[df_label_global['xlink_role'] == 'http://www.xbrl.org/2003/role/label']
    
    # 必要列のみに絞る
    df_label_global = df_label_global[['xlink_label', 'label']]
    
    # 'label_'はじまりを削除で統一
    df_label_global['xlink_label'] = df_label_global['xlink_label'].str.replace('label_', '')
    df_label_global.to_csv('df_label_global.csv')
    
    df_label_merged = df_label_global
    
    # 結合用ラベル列作成
    arg_fs['temp_label'] = arg_fs['account_item'].str.replace('[a-z]{3}-[a-z]{2}-[a-z]{1}:', '')
    arg_fs.to_csv('arg_fs.csv')

    # ラベルの結合
    df_labeled_fs = pd.merge(arg_fs, df_label_merged, left_on='temp_label', right_on='xlink_label', how='left').drop_duplicates()

    return df_labeled_fs

if __name__ == '__main__':
    
    uri = './sampleXBRLData/081220221013544202/XBRLData/Summary/tse-qcedjpsm-67550-20221013544202-ixbrl.htm'
    
    fs = get_fs(uri)
    
    raw_df = get_labeled_df(fs)
    raw_df.to_csv('raw_tdnet_df.csv')