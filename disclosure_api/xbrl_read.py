from bs4 import BeautifulSoup as bs
from bs4 import element as el
from pandas import DataFrame
import pandas as pd
import re
import zipfile
from decimal import Decimal
import warnings
from urllib.parse import urlparse
from pathlib import Path
import os
import requests
from datetime import datetime
import csv
import mojimoji

class MyException(Exception):
    
    def __init__(self, arg:str="") -> None:
        self.arg = arg
        
class XbrlValueNoneException(MyException):
    
    def __init__(self, arg: str = "") -> None:
        super().__init__(arg)
    
    def __str__(self) -> str:
        return f"XBRLタグ [{self.arg}] の値が存在しません。"
    
class NotCurrentDurationError(Exception):
    def __str__(self) -> str:
        return "参照したデータは本年分ではありません。"

class XbrlRead:
    def __init__(self, xbrl_zip_path) -> None:
        # 警告を非表示
        warnings.simplefilter("ignore")
        self.xbrl_zip_path = xbrl_zip_path
        self.taxonomy_files = self.load_label_xml()
        
    def label_df(self) -> DataFrame:
        # XBRLを読み込む
        df = self.to_dataframe()
        df.to_csv('D:/CSV/dataframe.csv')
        # データ構造のみコピー
        master_df = df[:0]
        master_df['label'] = None
        # namespace毎にグループ化
        group_df = df.groupby('namespace')
        for name, group in group_df:
            for file in self.taxonomy_files:
                
                # グローバルラベルリンク
                namespace = re.sub("_cor", "", name)
                if re.compile(f".*{namespace}.*lab.xml$").match(str(file).replace("\\", "/")):
                    tag_list = []
                    with open(file, mode='r', encoding='utf-8') as data:
                        soup = bs(data, 'lxml')
                        label_tag = soup.findAll(['link:label', 'label'])
                        for tag in label_tag:
                            tag_dict = {}
                            tag_dict['element'] = re.sub("^label_", "", tag.get('xlink:label'))
                            tag_dict['label'] = tag.text
                            tag_list.append(tag_dict)
                    global_label_df = DataFrame(tag_list)
                    group = pd.merge(group, global_label_df, how='left', on='element') 
                    master_df = pd.concat([master_df, group], axis=0)
                    
            # ローカルラベルリンク
            if re.compile("[a-z]{3}-[a-z]{8}-[0-9]{5}").match(name):
                with zipfile.ZipFile(self.xbrl_zip_path, mode='r') as zip_data:
                    for info in zip_data.infolist():
                        if re.compile("^.*lab.xml$").match(info.filename):
                            tag_list = []
                            soup = bs(zip_data.read(info.filename), 'lxml')
                            label_tag = soup.findAll(['link:label', 'label'])
                            for tag in label_tag:
                                tag_dict = {}
                                tag_dict['element'] = re.sub("^label_", "", tag.get('xlink:label'))
                                tag_dict['label'] = tag.text
                                tag_list.append(tag_dict)
                            global_label_df = DataFrame(tag_list)
                            group = pd.merge(group, global_label_df, how='left', on='element') 
                            master_df = pd.concat([master_df, group], axis=0)
        
        return pd.merge(df, master_df, how='left')
    
    def to_dataframe(self) -> DataFrame:
        
        dict_columns = ['reporting_date', 'code', 'period', '財表式別区分', '期区分', '連結・非連結区分', \
            '報告区分', '報告詳細区分', 'start_date', 'end_date', 'instant_date', \
            'namespace', 'element', 'context', 'unitref', 'format', 'numeric']
        df = None
        date_df = self.__context_date_df()
        company_datas = self.__Company_Data()
        
        # XBRLを取得
        with zipfile.ZipFile(self.xbrl_zip_path, mode='r') as zip_data:
            
            # サマリ、財務諸表の情報を読み取り
            for info in zip_data.infolist():
                if re.compile("^.*ixbrl.htm$").match(info.filename):
                    
                    list = []
                    soup = bs(zip_data.read(info.filename), 'lxml')
                    
                    # nonnumericタグを抽出                    
                    nonnumeric_tag = soup.find('ix:nonnumeric')
                    
                    # nonfractionタグを抽出
                    nonfraction_tag = soup.findAll('ix:nonfraction')
                    
                    # nonfractionタグの各要素を取得
                    for tag in nonfraction_tag:
                        try:
                            # 無効なタグは対象外
                            if tag.get('xsi:nil') == 'true':
                                raise XbrlValueNoneException("text_value")
                            
                            # 空の辞書を生成
                            dict_tag = dict.fromkeys(dict_columns, None)
                            
                            # 名前空間、属性、コンテキストを取得
                            tag_namespace = re.sub(":.*$", "", tag.get('name'))
                            tag_element = re.sub("^.*:", "", tag.get('name'))
                            tag_contextref = tag.get('contextref')                                                             
                            
                            # 開始日と終了日を取得
                            dict_tag['start_date'] = date_df[date_df['id'] == tag_contextref]['start_date'].iloc[-1]
                            dict_tag['end_date'] = date_df[date_df['id'] == tag_contextref]['end_date'].iloc[-1]
                            dict_tag['instant_date'] = date_df[date_df['id'] == tag_contextref]['instant_date'].iloc[-1]

                            # 会社情報を登録
                            dict_tag['reporting_date'] = company_datas['reporting_date']
                            dict_tag['code'] = company_datas['code']
                            dict_tag['period'] = company_datas['period']
                            
                            # 書類情報を登録
                            file_code = str(info.filename).split("/")
                            file_code = file_code[len(file_code)-1].split("-")
                            if re.compile("Attachment").search(info.filename):
                                dict_tag['財表式別区分'] = file_code[1]
                                report_cat = file_code[3]
                            else:
                                report_cat = file_code[1]
                            if len(report_cat) == 4:
                                dict_tag['報告区分'] = report_cat[0:4]
                            else:
                                dict_tag['期区分'] = report_cat[0] if re.compile("a|s|q").match(report_cat[0]) else None
                                dict_tag['連結・非連結区分'] = report_cat[1] if re.compile("c|n").match(report_cat[1]) else None
                                dict_tag['報告区分'] = report_cat[2:6]
                                dict_tag['報告詳細区分'] = report_cat[6:8]
                            
                            # 各項目を登録
                            dict_tag['namespace'] = tag_namespace
                            dict_tag['element'] = tag_element
                            dict_tag['context'] = tag_contextref
                            dict_tag['unitref'] = tag.get('unitref')
                            
                            if len(tag.contents) != 0:
                                dict_tag['format'] = re.sub("^.*:", "", tag.get('format'))
                                dict_tag['numeric'] = Decimal(re.sub(r"\D", "", tag.contents[0])) * 10 ** Decimal(tag.get('scale'))
                                
                                # 数値がマイナスの場合
                                if tag.get('sign') == '-':
                                    dict_tag['numeric'] = -1 * dict_tag['numeric']    
                                        
                            # 辞書をリストに追加
                            list.append(dict_tag)
                        except TypeError as identifier:
                            pass
                        except NotCurrentDurationError as identifier:
                            pass
                        except XbrlValueNoneException as identifier:
                            pass
                    
                    # DataFrameに変換
                    dict_df = DataFrame(list)
                    if df is None:
                        df = dict_df
                    else:
                        df = pd.concat([df, dict_df])
                            
        return df
        
    def load_label_xml(self) -> list:
        """リンクされているタクソノミをローカルフォルダに保存します。

            タクソノミファイルのローカルパスを出力します。

        Returns:
            list: タクソノミファイルパス
        """
        # 空のリスト
        list = []        
        
        # xsdファイルからスキーマ情報取得
        labelLink = []
        schemaLocation = []
        with zipfile.ZipFile(self.xbrl_zip_path, mode='r') as zip_data:
            for info in zip_data.infolist():
                if re.compile("^.*xsd$").match(info.filename):
                    
                    # XMLをスクレイピング
                    soup = bs(zip_data.read(info.filename), 'lxml')
                    
                    # labelLinkを取得
                    all_link = soup.find_all(attrs={'xlink:role': 'http://www.xbrl.org/2003/role/labelLinkbaseRef'})
                    for link in all_link:
                        if re.compile("^.*lab.xml$").match(link.get('xlink:href')):
                            # ラベルリンクをリストに追加
                            labelLink.append(link.get('xlink:href'))
                            
                    # schemaLocationを取得
                    tag_import = soup.find_all('xsd:import')
                    for data in tag_import:
                        schemaLocation.append(data.get('schemalocation'))   
        
        # labelLinkを保存
        for link in labelLink:
            # URLからパス部分を抽出
            url_path = urlparse(link).path.replace('/','\\')
            # ローカルパスに変換
            local_path = os.path.join(os.path.abspath(os.path.dirname(__file__)) + '\\doc\\taxonomy' + url_path)
            # ファイルの存在を問い合わせ
            is_file = os.path.isfile(local_path)
            # 存在しない場合、ローカルに保存x
            try:
                if is_file == False:
                    # リンクURLをリクエスト
                    response = requests.get(link)
                    # ディレクトリが存在しない場合新規作成
                    local_dir = Path(local_path).parent
                    if os.path.isdir(local_dir) == False:
                        os.makedirs(local_dir, exist_ok=True)
                    # ファイルの保存
                    with open(local_path, 'wb') as saveFile:
                        saveFile.write(response.content)
                list.append(local_path)
            except requests.exceptions.MissingSchema as identifier:
                pass     
        
        # schemaLocation
        for link in schemaLocation:
            # URLからパス部分を抽出
            url_path = urlparse(link).path.replace('/','\\')
            # ローカルパスに変換
            local_path = os.path.join(os.path.abspath(os.path.dirname(__file__)) + '\\doc\\taxonomy' + url_path)
            # ファイルの存在を問い合わせ
            is_file = os.path.isfile(local_path)
            # 存在しない場合、ローカルに保存
            try:
                if is_file == False:
                    # リンクURLをリクエスト
                    response = requests.get(link)
                    # ディレクトリが存在しない場合新規作成
                    local_dir = Path(local_path).parent
                    if os.path.isdir(local_dir) == False:
                        os.makedirs(local_dir, exist_ok=True)
                    # ファイルの保存
                    with open(local_path, 'wb') as saveFile:
                        saveFile.write(response.content)
                list.append(local_path)
            except requests.exceptions.MissingSchema as identifier:
                pass            
        
        return list 
        
    def __context_date_df(self) -> DataFrame:
        
        # 辞書のキーを定義
        dict_keys = ['id', 'start_date', 'end_date', 'instant_date']
        
        # 期間情報を格納する辞書
        date_list = []
        
        # XBRLを取得
        with zipfile.ZipFile(self.xbrl_zip_path, mode='r') as zip_data:           
            for info in zip_data.infolist():                    
                if re.compile(".*ixbrl.htm$").search(info.filename):    
                    
                    # BeautifulSoupで読み込む
                    soup = bs(zip_data.read(info.filename), 'lxml')                    
                        
                    # context<会計期間>タグを抽出
                    for context in soup.find_all('xbrli:context'):
                        # 格納用の辞書を作成
                        tag_dict = dict.fromkeys(dict_keys, None)
                        tag_dict['id'] = context.get('id')
                        tag_dict['start_date'] = None if context.find('xbrli:startdate') is None else context.find('xbrli:startdate').text
                        tag_dict['end_date'] = None if context.find('xbrli:enddate') is None else context.find('xbrli:enddate').text
                        tag_dict['instant_date'] = None if context.find('xbrli:instant') is None else context.find('xbrli:instant').text

                        # リストに追加
                        date_list.append(tag_dict)
                        
        df = DataFrame(date_list)
                    
        return df 
        
    def __Company_Data(self) -> dict:
        
        # 空の辞書を作成
        tag_dict = dict.fromkeys(['reporting_date', 'name', 'code', 'period'], None)
        
        # Zipファイルを展開する
        with zipfile.ZipFile(self.xbrl_zip_path, 'r') as zip_data:
            # ファイルを展開する
            for info in zip_data.infolist():
                # 対象のファイルを抽出
                if re.compile("tse-.*ixbrl.htm$|^.*Summary/.*ixbrl.htm$|^.*ixbrl.htm$").search(info.filename):
                    
                    # BeautifulSoupでスクレイピング
                    soup = bs(zip_data.read(info.filename), 'lxml')
                    
                    # 提出日
                    if tag_dict['reporting_date'] is None:
                        date_str = str(self.xbrl_zip_path).split("\\")[2]
                        tag_dict['reporting_date'] = datetime.strptime(date_str, "%Y%m%d").strftime("%Y-%m-%d")
                                            
                    # 会社名
                    if tag_dict['name'] is None:
                        company_name = soup.find('ix:nonnumeric', attrs={'name': [re.compile('^.*CompanyName'), re.compile('^.*AssetManagerREIT'), re.compile('FilerNameInJapaneseDEI')]})
                        tag_dict['name'] = company_name.text if company_name is not None else None
                    
                    # 銘柄コード
                    if tag_dict['code'] is None:
                        code = soup.find('ix:nonnumeric', attrs={'name': [re.compile('^.*SecuritiesCode'), re.compile('^.*SecurityCodeDEI')]})
                        tag_dict['code'] = code.text[0:4] if code is not None else None

                    # 会計期間
                    if tag_dict['period'] is None:
                        period = soup.find('ix:nonnumeric', attrs={'name':re.compile('^.*TypeOfCurrentPeriodDEI')})
                        period = period.text if period is not None else None
                        if period is not None:
                            if re.compile('Q[0-9]{1}').search(period):
                                tag_dict['period'] = int(re.compile("[0-9]{1}").search(period).group()) - 1 
                            elif period == 'FY':
                                tag_dict['period'] = 3
                                
        return tag_dict
                    
zip_path = Path("D:/ZIP/")
zip_list = list(zip_path.glob("**/*.zip"))
for zip_file in zip_list:
    
    print(zip_file)
    
    play = XbrlRead(zip_file)    
    df = play.label_df()    
    file_name = os.path.splitext(os.path.basename(zip_file))[0]
    df.to_csv(f'D:/CSV/label/{file_name}.csv')

# zip_path = "D:\\ZIP\\20221014\\081220220930538875.zip"
# play = XbrlRead(zip_path)
# df = play.label_df()
# file_name = os.path.splitext(os.path.basename(zip_path))[0]
# df.to_csv(f'D:/CSV/label/{file_name}.csv')