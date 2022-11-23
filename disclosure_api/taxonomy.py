import re
import pandas as pd
from bs4 import BeautifulSoup
import glob
import os
from pandas import DataFrame
import shutil
import warnings

class CreateTaxonomy:
    """タクソノミTSVファイルを生成するクラス
    """
    def __init__(self, input_path:str="./doc/taxonomy_raw/", output_path:str="./doc/taxonomy_tsv/") -> None:
        # 警告を非表示
        warnings.simplefilter("ignore")
        # 入力元・出力先フォルダのパスを初期化
        self.input_path = input_path
        self.output_path = output_path
        # 出力先フォルダを初期化
        if os.path.isdir(output_path) == True:
            shutil.rmtree(output_path)
        os.mkdir(output_path)
    
    def __attachment_taxonomy_df(self, arg_path) -> DataFrame:
        '''
        TDNETより配信されている適時開示XBRLのAttachmentフォルダ内(各財務諸表)に対応した
        タクソノミ[Dataframe]を取得します。
        '''
        # labファイルの読み込み
        with open(arg_path, encoding='utf-8') as f:
            soup = BeautifulSoup(f.read(), 'lxml')

        # link:locタグのみ抽出
        link_loc = soup.find_all('link:loc')

        # link:locタグの取得結果をdictにし、dictを格納するカラのリストを作成
        list_locator = []

        # link:locタグの情報をループ処理で取得
        for each_loc in link_loc:
            dict_locator = {}
            shema = each_loc.get('xlink:href').split(sep='#')[0]
            dict_locator['xmlns_jpcrp_ymd'] = re.findall(r'\d{4}-\d{2}-\d{2}', shema)[0]
            dict_locator['xlink_href'] = each_loc.get('xlink:href')
            dict_locator['shema'] = shema
            dict_locator['label_for_join'] = each_loc.get('xlink:href').split(sep='#')[1]
            dict_locator['loc_label'] = each_loc.get('xlink:label')
            list_locator.append(dict_locator)

        # link:locタグの取得結果をDFに    
        df_locator = pd.DataFrame(list_locator)

        # link:labelArcタグのみ抽出
        link_arc = soup.find_all('link:labelarc')

        # link:labelArcタグの取得結果をdictにし、dictを格納するカラのリストを作成
        list_arc = []

        # link:labelArcタグの情報をループ処理で取得
        for each_arc in link_arc:
            dict_arc = {}
            dict_arc['arc_role'] = each_arc.get('xlink:arcrole')
            dict_arc['loc_label'] = each_arc.get('xlink:from')
            dict_arc['xlink_label'] = each_arc.get('xlink:to')
            list_arc.append(dict_arc)

        # link:labelArcタグの取得結果をDFに    
        df_arc = pd.DataFrame(list_arc)

        # link:labelタグのみ抽出
        link_label = soup.find_all('link:label')

        # link:labelタグの取得結果をdictにし、dictを格納するカラのリストを作成
        list_label = []

        # link:labelタグの情報をループ処理で取得
        for each_label in link_label:
            dict_label = {}
            dict_label['xlink_label'] = each_label.get('xlink:label')
            dict_label['xlink_role'] = each_label.get('xlink:role')
            dict_label['xml_lang'] = each_label.get('xml:lang')
            dict_label['label_text'] = each_label.text
            list_label.append(dict_label)

        # link:labelタグの取得結果をDFに    
        df_label = pd.DataFrame(list_label)

        # locとarcの結合
        df_merged = pd.merge(df_locator, df_arc, on='loc_label', how='inner')
        # loc, arcとlabelの結合
        df_merged = pd.merge(df_merged, df_label, on='xlink_label', how='inner')

        return df_merged

    def __summary_taxonomy_df(self, arg_path) -> DataFrame:
        '''
        TDNETより配信されている適時開示XBRLのSummaryフォルダ内(決算短信等)に対応した
        タクソノミ[Dataframe]を取得します。
        '''
        # labファイルの読み込み
        with open(arg_path, encoding='utf-8') as f:
            soup = BeautifulSoup(f.read(), 'lxml')

        # link:locタグのみ抽出
        link_loc = soup.find_all('loc')

        # link:locタグの取得結果をdictにし、dictを格納するカラのリストを作成
        list_locator = []

        # link:locタグの情報をループ処理で取得
        for each_loc in link_loc:
            dict_locator = {}
            shema = each_loc.get('xlink:href').split(sep='#')[0]
            dict_locator['xmlns_jpcrp_ymd'] = re.findall(r'\d{4}-\d{2}-\d{2}', shema)[0]
            dict_locator['xlink_href'] = each_loc.get('xlink:href')
            dict_locator['shema'] = shema
            dict_locator['label_for_join'] = each_loc.get('xlink:href').split(sep='#')[1]
            dict_locator['loc_label'] = each_loc.get('xlink:label')
            list_locator.append(dict_locator)

        # link:locタグの取得結果をDFに    
        df_locator = pd.DataFrame(list_locator)

        # link:labelArcタグのみ抽出
        link_arc = soup.find_all('labelarc')

        # link:labelArcタグの取得結果をdictにし、dictを格納するカラのリストを作成
        list_arc = []

        # link:labelArcタグの情報をループ処理で取得
        for each_arc in link_arc:
            dict_arc = {}
            dict_arc['arc_role'] = each_arc.get('xlink:arcrole')
            dict_arc['loc_label'] = each_arc.get('xlink:from')
            dict_arc['xlink_label'] = each_arc.get('xlink:to')
            list_arc.append(dict_arc)

        # link:labelArcタグの取得結果をDFに    
        df_arc = pd.DataFrame(list_arc)

        # link:labelタグのみ抽出
        link_label = soup.find_all('label')

        # link:labelタグの取得結果をdictにし、dictを格納するカラのリストを作成
        list_label = []

        # link:labelタグの情報をループ処理で取得
        for each_label in link_label:
            dict_label = {}
            dict_label['xlink_label'] = each_label.get('xlink:label')
            dict_label['xlink_role'] = each_label.get('xlink:role')
            dict_label['xml_lang'] = each_label.get('xml:lang')
            dict_label['label_text'] = each_label.text
            list_label.append(dict_label)

        # link:labelタグの取得結果をDFに    
        df_label = pd.DataFrame(list_label)
        df_locator.to_csv('df_locator.csv')
        df_arc.to_csv('df_arc.csv')
        # locとarcの結合
        df_merged = pd.merge(df_locator, df_arc, on='loc_label', how='inner')
        # loc, arcとlabelの結合
        df_merged = pd.merge(df_merged, df_label, on='xlink_label', how='inner')

        return df_merged
        
    def attachment_taxonomy_tsv(self) -> bool:
        
        # 各labファイルのパスを格納するリストの定義
        list_path_taxonomy = []

        # 各labファイルの検索
        list_path_taxonomy.extend(glob.glob(self.input_path + '**/jpcrp/**/label/**_lab.xml', recursive=True)) # 日本基準用タクソノミ
        list_path_taxonomy.extend(glob.glob(self.input_path + '**/jppfs/**/label/**_lab.xml', recursive=True)) # IFRS用タクソノミ
        list_path_taxonomy.extend(glob.glob(self.input_path + '**/jpigp/**/label/**_lab.xml', recursive=True)) # 米国基準用タクソノミ
        
        for filePath in list_path_taxonomy:
            
            df_global_label = self.__attachment_taxonomy_df(filePath)

            ########## TSVファイルの保存名を指定 ##########
            filename = 'attachment_taxonomy.tsv'

            # TSVファイルにタクソノミを書き出す
            df_global_label.to_csv(self.output_path + filename, sep ='\t', encoding='UTF-8', mode='a', header=False, index=False)

        return os.path.isfile(self.output_path + filename)
        
    def summary_taxonomy_tsv(self) -> bool:
        
        # 各labファイルのパスを格納するリストの定義
        list_path_taxonomy = []

        # 各labファイルの検索
        list_path_taxonomy.extend(glob.glob(self.input_path + 'tdnet/**-lab.xml', recursive=True)) # 決算短信用タクソノミ
        
        for filePath in list_path_taxonomy:
            
            df_global_label = self.__summary_taxonomy_df(filePath)

            ########## TSVファイルの保存名を指定 ##########
            filename = 'summary_taxonomy.tsv'

            # TSVファイルにタクソノミを書き出す
            df_global_label.to_csv(self.output_path + filename, sep ='\t', encoding='UTF-8', mode='a', header=False, index=False)

        return os.path.isfile(self.output_path + filename)
