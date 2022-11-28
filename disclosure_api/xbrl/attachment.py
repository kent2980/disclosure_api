import pandas as pd
from pandas import DataFrame
from bs4 import BeautifulSoup
from os import path
from xbrl.__IXbrlCollection import ITdnetCollection
import warnings

class Attachment(ITdnetCollection):
    """TDNETより取得したXBRLデータのうち、
    Attachmentフォルダ(財務諸表)から情報を取得するクラス。

    Args:
        ITdnetCollection (_type_): TDNET基底クラス
    """
    def __init__(self, arg_data, path_taxonomy_labels, arg_local_taxonomy_data) -> None:
        super().__init__(arg_data, path_taxonomy_labels)
        # 警告を非表示
        warnings.simplefilter("ignore")
        self.local_taxonomy_data = arg_local_taxonomy_data

    def get_fs(self, arg_data) -> DataFrame:
        # fsファイルの読み込み。bs4でパース
        soup = BeautifulSoup(arg_data, 'lxml')

        # nonNumericタグのみ抽出
        tags_nonnumeric = soup.find_all('ix:nonnumeric')

        # nonnumericの各要素を格納するカラの辞書を作成
        dict_tag = {}
        # nonnumericの内容を辞書型に
        for tag in tags_nonnumeric:
            dict_tag[tag.get('name')] = tag

        # 各財務諸表を入れるカラのDFを作成        
        list_fs = []
        # 財務諸表要素は'ix:nonFraction'に入っているため、このタグを取得
        tag_nonfraction = soup.find_all('ix:nonfraction')
        # 財務諸表の各要素をDFに。財務諸表区分とタグを引数にして関数に渡す。
        df_each_fs = self.get_df(tag_nonfraction)
        list_fs.append(df_each_fs)

        # タグの中にtarget_FSが含まれない場合（例えば注記だけのixbrlを読み込んだ場合）の分岐
        if list_fs:
            # 各財務諸表の結合
            df_fs = pd.concat(list_fs)

            # 並べ替え
            df_fs = df_fs[['account_item', 'contextRef', 'format', 'decimals', 'scale', 'unitRef', 'amount']]

        else:
            df_fs = pd.DataFrame(index=[])

        return df_fs

    def get_labeled_df(self) -> DataFrame:
        
        # arg_fsの読み込み
        arg_fs = self.get_fs(self.data)
        # arg_label_localの読み込み
        arg_label_local = self.__get_label_local(self.local_taxonomy_data)
        
        # 標準ラベルの読み込み
        df_label_global = pd.read_table(self.taxonomy_labels, sep='\t', encoding='utf-8')
        # 標準ラベルの処理
        
        # 標準ラベルデータのうち、必要行に絞る
        df_label_global = df_label_global[df_label_global['xlink_role'] == 'http://www.xbrl.org/2003/role/label']
        
        # 必要列のみに絞る
        df_label_global = df_label_global[['xlink_label', 'label']]
        
        # 'label_'はじまりを削除で統一
        df_label_global['xlink_label'] = df_label_global['xlink_label'].str.replace('label_', '')
        
        # 同一ラベルで異なる表示名が存在する場合、独自の表示名を優先
        df_label_global['temp'] = 0
        
        # 標準ラベルのみ使用し、独自ラベルを持たない場合があるため、if分岐
        if arg_label_local is not None and len(arg_label_local) > 0:
            # 独自ラベルの処理
            
            # 独自ラベルデータのうち、必要列に絞る
            df_label_local = arg_label_local[['xlink_label', 'label']].copy()
            
            # ラベルの末尾に'_label.*'があり、FSと結合できないため、これを削除
            df_label_local['xlink_label'] = df_label_local['xlink_label'].str.replace('_label.*$', '').copy()
            
            # ラベルの最初に'jpcrp'で始まるラベルがあり、削除で統一。
            
            # 削除で統一した結果、各社で定義していた汎用的な科目名（「貸借対照表計上額」など）が重複するようになる。後続処理で重複削除。
            df_label_local['xlink_label'] = df_label_local['xlink_label'].str.replace('[a-z]{5}_cor_', '')
            df_label_local['xlink_label'] = df_label_local['xlink_label'].str.replace('[a-z]{3}-[a-z]{8}-[0-9]{5}_', '')
            
            # ラベルの最初に'label_'で始まるラベルがあり、削除で統一
            
            # 削除で統一した結果、各社で定義していた汎用的な科目名（「貸借対照表計上額」など）が重複するようになる。後続処理で重複削除。
            df_label_local['xlink_label'] = df_label_local['xlink_label'].str.replace('label_', '')
            
            # 同一要素名で異なる表示名が存在する場合、独ラベルを優先
            df_label_local['temp'] = 1

            # label_globalとlabel_localを縦結合
            df_label_merged = pd.concat([df_label_global, df_label_local])
        else:
            df_label_merged = df_label_global
        

        # 同一要素名で異なる表示名が存在する場合、独自ラベルを優先
        grp_df_label_merged = df_label_merged.groupby('xlink_label')
        df_label_merged = df_label_merged.loc[grp_df_label_merged['temp'].idxmax(),:]
        df_label_merged = df_label_merged.drop('temp', axis=1)

        # localラベルで重複してしまう行があるため、ここで重複行を削除
        df_label_merged = df_label_merged.drop_duplicates()

        # 結合用ラベル列作成
        arg_fs['temp_label'] = arg_fs['account_item'].str.replace('[a-z]{5}_cor:', '')
        arg_fs['temp_label'] = arg_fs['temp_label'].str.replace('[a-z]{5}_cor:', '')
        arg_fs['temp_label'] = arg_fs['temp_label'].str.replace('[a-z]{3}-[a-z]{8}-[0-9]{5}:', '')

        # ラベルの結合
        df_labeled_fs = pd.merge(arg_fs, df_label_merged, left_on='temp_label', right_on='xlink_label', how='left').drop_duplicates()

        return df_labeled_fs

    def __get_label_local(self, arg_data) -> DataFrame:
        """独自のXBRLラベルを取得してDataFrameを返します

        Args:
            arg_data (_type_): ローカルラベルのxmlデータ

        Returns:
            DataFrame: ローカルラベルのDataFrame
        """
        
        if arg_data is not None:
            # labファイルの読み込み
            soup = BeautifulSoup(arg_data, 'lxml')

            # link:labelタグのみ抽出
            link_label = soup.find_all('link:label')

            # ラベル情報用dictを格納するカラのリストを作成
            list_label = []

            # ラベル情報をループ処理で取得
            for each_label in link_label:
                dict_label = {}
                dict_label['id'] = each_label.get('id')
                dict_label['xlink_label'] = each_label.get('xlink:label')
                dict_label['xlink_role'] = each_label.get('xlink:role')
                dict_label['xlink_type'] = each_label.get('xlink:type')
                dict_label['xml_lang'] = each_label.get('xml:lang')
                dict_label['label'] = each_label.text
                list_label.append(dict_label)

            # ラベル情報取得結果をDFに    
            df_label_local = pd.DataFrame(list_label)
        
            return df_label_local