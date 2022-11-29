import pandas as pd
from bs4 import BeautifulSoup
import warnings
from disclosure_api.xbrl.__IXbrlCollection import ITdnetCollection

class Summary(ITdnetCollection):
    
    def __init__(self, arg_data, path_taxonomy_labels) -> None:
        # 警告を非表示
        warnings.simplefilter("ignore")
        super().__init__(arg_data, path_taxonomy_labels)

    def get_fs(self, arg_data):
        # fsファイルの読み込み。bs4でパース
        soup = BeautifulSoup(arg_data, 'lxml')

        # 財務諸表要素は'ix:nonFraction'に入っているため、このタグを取得
        tag_nonfraction = soup.find_all('ix:nonfraction')
        # 財務諸表の各要素をDFに。財務諸表区分とタグを引数にして関数に渡す。
        df_each_fs = self.get_df(tag_nonfraction)
        
        # 並べ替え
        df_each_fs = df_each_fs[['account_item', 'contextRef', 'format', 'decimals', 'scale', 'unitref', 'amount']]

        # 金額が空白の行は削除
        df_each_fs = df_each_fs.drop(df_each_fs[df_each_fs['amount'] == ""].index)

        return df_each_fs

    def get_labeled_df(self):
        
        # DataFrameの読み込み
        arg_fs = self.get_fs(self.data)
        
        # 標準ラベルの読み込み
        df_label_global = pd.read_table(self.taxonomy_labels, sep='\t', encoding='utf-8')
        # 標準ラベルの処理
        
        # 標準ラベルデータのうち、必要行に絞る
        df_label_global = df_label_global[df_label_global['xlink_role'] == 'http://www.xbrl.org/2003/role/label']
        
        # 必要列のみに絞る
        df_label_global = df_label_global[['xlink_label', 'label']]
        
        # 'label_'はじまりを削除で統一
        df_label_global['xlink_label'] = df_label_global['xlink_label'].str.replace('label_', '')        
        df_label_merged = df_label_global
        
        # 結合用ラベル列作成
        arg_fs['temp_label'] = arg_fs['account_item'].str.replace('[a-z]{3}-[a-z]{2}-[a-z]{1}:', '')

        # ラベルの結合
        df_labeled_fs = pd.merge(arg_fs, df_label_merged, left_on='temp_label', right_on='xlink_label', how='left').drop_duplicates()
        # temp_label,account_item列を削除
        df_labeled_fs = df_labeled_fs.drop(["temp_label", "account_item"], axis=1)
        # 列の並び替え
        df_labeled_fs = df_labeled_fs[["xlink_label", "contextRef", "label", "format", "decimals", "scale", "unitref", "amount"]]

        
        return df_labeled_fs
    