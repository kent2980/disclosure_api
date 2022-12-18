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
                    
                    # 書類属性を取得
                    doc_namespace = re.sub(":.*$", "", nonnumeric_tag.get('name'))
                    doc_element = re.sub("^.*:", "", nonnumeric_tag.get('name'))
                    doc_contextref = nonnumeric_tag.get('contextref')
                    
                    # nonfractionタグを抽出
                    nonfraction_tag = soup.findAll('ix:nonfraction')
                    
                    # nonfractionタグの各要素を取得
                    for tag in nonfraction_tag:
                        try:
                            # 無効なタグは対象外
                            if tag.get('xsi:nil') == 'true':
                                raise XbrlValueNoneException("text_value")
                            
                            # 空の辞書を生成
                            dict_tag = {}
                            
                            # 名前空間、属性、コンテキストを取得
                            tag_namespace = re.sub(":.*$", "", tag.get('name'))
                            tag_element = re.sub("^.*:", "", tag.get('name'))
                            tag_contextref = tag.get('contextref')                                 
                            
                            # 過去のデータを参照した場合は例外発生
                            if re.compile("Current").match(tag_contextref) is None:
                                raise NotCurrentDurationError()
                            
                            # 会社情報を登録
                            dict_tag['reporting_date'] = company_datas['reporting_date']
                            dict_tag['code'] = company_datas['code']
                            
                            # 開始日と終了日を取得
                            match tag_contextref:
                                #当四半期累計期間
                                case "CurrentYTDDuration": 
                                    dict_tag['start_date'] = date_df[date_df['id'].str.contains("CurrentAccumulatedQ[1-4]{1}Duration_.*$")]['start_date'].iloc[-1]
                                    dict_tag['end_date'] = date_df[date_df['id'].str.contains("CurrentAccumulatedQ[1-4]{1}Duration_.*$")]['end_date'].iloc[-1]
                                    dict_tag['instant_date'] = date_df[date_df['id'].str.contains("CurrentAccumulatedQ[1-4]{1}Duration_.*$")]['instant_date'].iloc[-1] 
                                #当四半期会計期間
                                case "CurrentQuarterInstant": 
                                    dict_tag['start_date'] = date_df[date_df['id'].str.contains("CurrentAccumulatedQ[1-4]{1}Instant_.*$")]['start_date'].iloc[-1]
                                    dict_tag['end_date'] = date_df[date_df['id'].str.contains("CurrentAccumulatedQ[1-4]{1}Instant_.*$")]['end_date'].iloc[-1]
                                    dict_tag['instant_date'] = date_df[date_df['id'].str.contains("CurrentAccumulatedQ[1-4]{1}Instant_.*$")]['instant_date'].iloc[-1]                                    
                                case "CurrentYearDuration":
                                    dict_tag['start_date'] = date_df[date_df['id'].str.contains("CurrentYearDuration_.*$")]['start_date'].iloc[-1]
                                    dict_tag['end_date'] = date_df[date_df['id'].str.contains("CurrentYearDuration_.*$")]['end_date'].iloc[-1]
                                    dict_tag['instant_date'] = date_df[date_df['id'].str.contains("CurrentYearDuration_.*$")]['instant_date'].iloc[-1]   
                                #通常処理
                                case _: 
                                    dict_tag['start_date'] = date_df[date_df['id'] == tag_contextref]['start_date'].iloc[-1]
                                    dict_tag['end_date'] = date_df[date_df['id'] == tag_contextref]['end_date'].iloc[-1]
                                    dict_tag['instant_date'] = date_df[date_df['id'] == tag_contextref]['instant_date'].iloc[-1] 
                                
                            # 各要素を辞書に追加
                            dict_tag['document_namespace'] = doc_namespace
                            dict_tag['document_element'] = doc_element
                            dict_tag['document_context'] = doc_contextref
                            dict_tag['namespace'] = tag_namespace
                            dict_tag['element'] = tag_element
                            dict_tag['context'] = tag_contextref
                            dict_tag['unitref'] = tag.get('unitref')
                            
                            if len(tag.contents) != 0:
                                dict_tag['format'] = re.sub("^.*:", "", tag.get('format'))
                                dict_tag['numeric'] = Decimal(re.sub(",", "", tag.contents[0])) * 10 ** Decimal(tag.get('scale'))
                                
                                # 数値がマイナスの場合
                                if tag.get('sign') == '-':
                                    dict_tag['numeric'] = -1 * dict_tag['numeric']    
                                        
                            # 辞書をリストに追加
                            list.append(dict_tag)
                        except TypeError as identifier:
                            pass
                        except NotCurrentDurationError:
                            pass
                        except XbrlValueNoneException:
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
        
        # 期間情報を格納する辞書
        date_list = []
        
        # XBRLを取得
        with zipfile.ZipFile(self.xbrl_zip_path, mode='r') as zip_data:
            
            # サマリより共通データを読み取り
            for info in zip_data.infolist():
                    
                if re.compile("^tse-.*ixbrl.htm$|^.*Summary/.*ixbrl.htm$").match(info.filename):    
                    
                    # BeautifulSoupで読み込む
                    soup = bs(zip_data.read(info.filename), 'lxml')                    
                        
                    # context<会計期間>タグを抽出
                    for context in soup.find_all('xbrli:context'):
                        
                        # idを取得
                        context_id = context.get('id')
                        context_id = re.sub("_.*$", "", context_id)
                        
                        try:
                            if context is None:
                                raise XbrlValueNoneException('xbrli:context')
                            
                            for context_data in context.find_all('xbrli:period'):
                                
                                if context_data is None:
                                    raise XbrlValueNoneException('xbrli:period')
                                
                                # 対象期間の詳細を格納する辞書
                                date_dict = {}    
                                
                                # 辞書にidを登録
                                date_dict['id'] = context_id
                                
                                # 要素を取得
                                start_date = context_data.find('xbrli:startdate')
                                end_date = context_data.find('xbrli:enddate')
                                instant_date = context_data.find('xbrli:instant')
                                date_dict['start_date'] = None if start_date is None else start_date.text
                                date_dict['end_date'] = None if end_date is None else end_date.text
                                date_dict['instant_date'] = None if instant_date is None else instant_date.text
                                
                                if start_date is None and end_date is None and instant_date is None:
                                    raise XbrlValueNoneException('all date tag')
                                
                                # 詳細データを辞書に格納
                                date_list.append(date_dict)
                        except XbrlValueNoneException as identifier:
                            pass 
                    
        date_df = DataFrame(date_list).drop_duplicates(subset='id')
        date_df.to_csv("D:/CSV/date_df.csv")
        return date_df 
        
    def __Company_Data(self) -> dict:
        
        tag_dict = {}
        
        with zipfile.ZipFile(self.xbrl_zip_path, 'r') as zip_data:
            
            for info in zip_data.infolist():
                if re.compile("^tse-.*ixbrl.htm$|^.*Summary/.*ixbrl.htm$").match(info.filename):
                    try:
                        soup = bs(zip_data.read(info.filename), 'lxml')
                        
                        tag_dict['reporting_date'] = soup.find('ix:nonnumeric', \
                            attrs={'name':['tse-ed-t:ReportingDateOfFinancialForecastCorrection','tse-ed-t:FilingDate']}).text
                        tag_dict['reporting_date'] = datetime.strptime(tag_dict['reporting_date'], "%Y年%m月%d日").date().strftime("%Y-%m-%d")
                        tag_dict['name'] = soup.find('ix:nonnumeric', attrs={'name':'tse-ed-t:CompanyName'}).text
                        tag_dict['code'] = soup.find('ix:nonnumeric', attrs={'name':'tse-ed-t:SecuritiesCode'}).text[0:4]
                    except AttributeError as identifier:
                        pass
                    
        return tag_dict
                    
zip_path = Path("D:/ZIP/")
zip_list = list(zip_path.glob("**/*.zip"))
for zip_file in zip_list:
    
    print(zip_file)
    
    play = XbrlRead(zip_file)    
    df = play.label_df()    
    file_name = os.path.splitext(os.path.basename(zip_file))[0]
    df.to_csv(f'D:/CSV/label/{file_name}.csv')