from bs4 import BeautifulSoup as bs
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
import pymysql.cursors
import json

class MyException(Exception):
    """自作例外クラスの基底クラス

    Args:
        Exception (_type_): エラーメッセージ
    """

    def __init__(self, arg: str = "") -> None:
        self.arg = arg


class XbrlValueNoneException(MyException):
    """参照したXBRLタグが存在しない場合に発生する例外

    Args:
        MyException (_type_): 自作例外クラス
    """

    def __init__(self, arg: str = "") -> None:
        super().__init__(arg)

    def __str__(self) -> str:
        return f"XBRLタグ [{self.arg}] の値が存在しません。"

class NoneXbrlZipPathSetting(Exception):
    """ZIPファイルのパスが未設定の場合に発生する例外

    Args:
        Exception (_type_): 例外基底クラス
    """
    
    def __str__(self) -> str:
        return "ZIPファイルのパスを指定してください。"


class XbrlRead:
    """TDNETより配信されたXBRLの読み込み機能を提供します。

    XBRLのZIPファイルのパスを設定してください。

    Args:
        xbrl_zip_path (str, optional): ZIP(XBRL) ファイルパス

    Raises:
        NoneXbrlZipPathSetting: パス未設定エラー
        
    Examples:
    
        初期化：TDNETからダウンロードしたZIPファイルのパスを読み込んでください。
        
        >>> zip_path = " ***/***/********.zip"
            x = XbrlRead(zip_path)
            
        (a) XBRLのDataFrameを出力
        
        >>> xbrl_df = x.to_dataframe()
        
        (b) XBRLの勘定科目ラベル付きDataFrameを出力
        
        >>> xbrl_df = x.add_label_df()
        
        (c) XBRLの計算リンクを出力 -> DataFrame
        
        >>> cal_df = x.to_cal_link_df()
        
        (d) XBRLの定義リンクを出力 -> DataFrame
        
        >>> def_df = x.to_def_link_df()
        
        (e) XBRLの表示リンクを出力 -> DataFrame
        
        >>> pre_df = x.to_pre_link_df()
        
    """

    def __init__(self, xbrl_zip_path: str = None) -> None:
        """TDNETより配信されたXBRLの読み込み機能を提供します。

        XBRLのZIPファイルのパスを設定してください。

        Args:
            xbrl_zip_path (str, optional): ZIP(XBRL) ファイルパス

        Raises:
            NoneXbrlZipPathSetting: パス未設定エラー
            
        Examples:
        
            初期化：TDNETからダウンロードしたZIPファイルのパスを読み込んでください。
            
            >>> zip_path = " ***/***/********.zip"
                x = XbrlRead(zip_path)
                
            (a) XBRLのDataFrameを出力
            
            >>> xbrl_df = x.to_dataframe()
            
            (b) XBRLの勘定科目ラベル付きDataFrameを出力
            
            >>> xbrl_df = x.add_label_df()
            
            (c) XBRLの計算リンクを出力 -> DataFrame
            
            >>> cal_df = x.to_cal_link_df()
            
            (d) XBRLの定義リンクを出力 -> DataFrame
            
            >>> def_df = x.to_def_link_df()
            
            (e) XBRLの表示リンクを出力 -> DataFrame
            
            >>> pre_df = x.to_pre_link_df()
            
        """

        # ZIPファイルのパスが未設定の場合に例外発生
        if xbrl_zip_path is None:
            raise NoneXbrlZipPathSetting()

        # 警告を非表示
        warnings.simplefilter("ignore")
        # ZIPファイルのパスを設定
        self.xbrl_zip_path = xbrl_zip_path
        self.xbrl_df = self.to_dataframe()

    def add_label_df(self) -> DataFrame:
        """勘定科目ラベルを付与した報告書・財務諸表のDataFrameを出力します。

        Returns:
            DataFrame: 勘定科目ラベル付き（報告書・財務）情報
        """

        # ************************************************
        # インナー関数 ************************************
        # ************************************************

        def load_global_label_xml() -> list:
            """グローバルラベルファイルのローカルパスを出力します。

            ローカルに存在しない場合は自動取得します。

            Returns:
                list: リンクファイル一覧
            """

            # 空のリスト
            dict_list = []

            # xsdファイルからスキーマ情報取得
            labelLink = []
            schemaLocation = []
            with zipfile.ZipFile(self.xbrl_zip_path, mode='r') as zip_data:
                for info in zip_data.infolist():
                    if re.compile("^.*xsd$").match(info.filename):

                        # XMLをスクレイピング
                        soup = bs(zip_data.read(info.filename), 'lxml')

                        # labelLinkを取得
                        all_link = soup.find_all(
                            attrs={'xlink:role': 'http://www.xbrl.org/2003/role/labelLinkbaseRef'})
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
                url_path = urlparse(link).path.replace('/', '\\')
                # ローカルパスに変換
                local_path = os.path.join(os.path.abspath(
                    os.path.dirname(__file__)) + '\\doc\\taxonomy' + url_path)
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
                    dict_list.append(local_path)
                except requests.exceptions.MissingSchema as identifier:
                    pass

            # schemaLocation
            for link in schemaLocation:
                # URLからパス部分を抽出
                url_path = urlparse(link).path.replace('/', '\\')
                # ローカルパスに変換
                local_path = os.path.join(os.path.abspath(
                    os.path.dirname(__file__)) + '\\doc\\taxonomy' + url_path)
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
                    dict_list.append(local_path)
                except requests.exceptions.MissingSchema as identifier:
                    pass

            return list(set(dict_list))

        def add_label_link(df: DataFrame, global_label_xml: list) -> DataFrame:
            """報告書・財務情報に勘定ラベルを付与します

            Args:
                df (DataFrame): 報告書・財務情報

            Returns:
                DataFrame: 勘定ラベル付き（報告書・財務）情報
            """

            # データ構造のみコピー
            tag_df = df[:0]
            tag_df['label'] = None

            # namespace毎にグループ化
            group_df = df.groupby('namespace')
            for name, group in group_df:

                # *************************************************************
                # グローバルラベルリンク ****************************************
                # *************************************************************

                # タクソノミファイルを展開
                for file in global_label_xml:
                    # 名前空間を抽出
                    namespace = re.sub("_cor", "", name)
                    # 対象のタクソノミファイルで処理を行う
                    if re.compile(f".*{namespace}.*lab.xml$").match(str(file).replace("\\", "/")):

                        # 空のリスト
                        tag_list = []

                        # タクソノミファイルを開く
                        with open(file, mode='r', encoding='utf-8') as data:

                            # タクソノミファイル(xml)をスクレイピング
                            soup = bs(data, 'lxml')
                            # ラベルタグを全て抽出
                            label_tag = soup.findAll(['link:label', 'label'])

                            # タグを順番に処理
                            for tag in label_tag:

                                # 空の辞書
                                tag_dict = {}
                                # elementを抽出
                                tag_dict['element'] = re.sub(
                                    "^label_", "", tag.get('xlink:label'))
                                # labelを抽出
                                tag_dict['label'] = tag.text
                                # リストに辞書を追加
                                tag_list.append(tag_dict)

                        # ラベルリストをDataFrameに変換
                        global_label_df = DataFrame(tag_list)
                        # グローラベルのnamespaceにラベルを付与
                        group = pd.merge(group, global_label_df,
                                         how='left', on='element')
                        # マスタにラベル付きデータを追加
                        tag_df = pd.concat([tag_df, group], axis=0)

                # **************************************************************
                # ローカルラベルリンク *******************************************
                # **************************************************************
                if re.compile("tse-[a-z]{8}-[0-9]{5}").search(name):
                    with zipfile.ZipFile(self.xbrl_zip_path, mode='r') as zip_data:

                        file_info = list(zip_data.infolist())
                        file_info.sort(key=lambda x: x.filename, reverse=True)
                        for info in file_info:
                            if re.compile("^.*lab.xml$").match(info.filename):

                                # 空のリスト
                                tag_list = []
                                # ローカルラベル(xml)をスクレイピング
                                soup = bs(zip_data.read(info.filename), 'lxml')
                                # link:label タグを抽出
                                label_tag = soup.findAll(
                                    ['link:label', 'label'])
                                # namespaceを取得
                                namespace = re.compile(
                                    "tse-[a-z]{8}-[0-9]{5}").search(name).group()
                                # タグを取り出す
                                for tag in label_tag:

                                    # 空の辞書
                                    tag_dict = {}
                                    # elementを抽出
                                    element = tag.get('xlink:label')
                                    element = re.sub("^label_", "", element)
                                    element = re.sub("_label$", "", element)
                                    element = re.sub(
                                        f"{namespace}_", "", element)
                                    tag_dict['element'] = element
                                    # labelを抽出
                                    tag_dict['label'] = tag.text
                                    # リストに辞書を追加
                                    tag_list.append(tag_dict)

                                break

                        # ラベルリストをDataFrameに変換
                        local_label_df = DataFrame(tag_list)
                        # ローカルのnamespaceにラベルを付与
                        group = pd.merge(group, local_label_df,
                                         how='left', on='element')
                        # マスタにラベル付きデータを追加
                        tag_df = pd.concat([tag_df, group], axis=0)

            # すべての（財務諸表・報告書）とラベル情報を統合する
            master_df = pd.merge(df, tag_df, how='left')

            return master_df

        # ************************************************
        # ここから処理の記述を開始... **********************
        # ************************************************

        # グローバルラベルを読み込む
        global_label_xml = load_global_label_xml()

        # XBRLを読み込む
        xbrl_df = self.xbrl_df

        # 勘定ラベルを付与
        master_df = add_label_link(xbrl_df, global_label_xml)
        
        # 重複業を削除
        master_df = master_df.drop_duplicates()

        return master_df

    def to_dataframe(self) -> DataFrame:
        """報告書と財務情報を出力します。

        Returns:
            DataFrame: 報告書・財務情報
        """

        def context_date_df() -> DataFrame:
            """各コンテキストの期間データ一覧を出力します。

            Returns:
                DataFrame: 期間データ一覧
            """

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
                            tag_dict['start_date'] = None if context.find(
                                'xbrli:startdate') is None else context.find('xbrli:startdate').text
                            tag_dict['end_date'] = None if context.find(
                                'xbrli:enddate') is None else context.find('xbrli:enddate').text
                            tag_dict['instant_date'] = None if context.find(
                                'xbrli:instant') is None else context.find('xbrli:instant').text

                            # リストに追加
                            date_list.append(tag_dict)

            df = DataFrame(date_list)

            return df

        def company_data_dict() -> dict:
            """会社の固有情報を出力します。

            Returns:
                dict: 会社固有情報
            """

            # 空の辞書を作成
            tag_dict = dict.fromkeys(
                ['reporting_date', 'name', 'code', 'period'], None)

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
                            date_str = re.compile("[0-9]{8}").search(str(self.xbrl_zip_path)).group()
                            tag_dict['reporting_date'] = datetime.strptime(
                                date_str, "%Y%m%d").strftime("%Y-%m-%d")

                        # 会社名
                        if tag_dict['name'] is None:
                            company_name = soup.find('ix:nonnumeric', attrs={'name': [re.compile(
                                '^.*CompanyName'), re.compile('^.*AssetManagerREIT'), re.compile('FilerNameInJapaneseDEI')]})
                            tag_dict['name'] = company_name.text if company_name is not None else None

                        # 銘柄コード
                        if tag_dict['code'] is None:
                            code = soup.find('ix:nonnumeric', attrs={'name': [re.compile(
                                '^.*SecuritiesCode'), re.compile('^.*SecurityCodeDEI')]})
                            tag_dict['code'] = code.text[0:4] if code is not None else None

                        # 会計期間
                        if tag_dict['period'] is None:
                            period = soup.find('ix:nonnumeric', attrs={
                                               'name': re.compile('^.*TypeOfCurrentPeriodDEI')})
                            period = period.text if period is not None else None
                            if period is not None:
                                if re.compile('Q[0-9]{1}').search(period):
                                    tag_dict['period'] = int(re.compile(
                                        "[0-9]{1}").search(period).group()) - 1
                                elif period == 'FY':
                                    tag_dict['period'] = 3
                                elif period == 'HY':
                                    tag_dict['period'] = 1

            return tag_dict

        # 辞書のキーを定義する
        dict_columns = ['reporting_date', 'code', 'period', 'doc_element', 'doc_label', 'financial_statement', 'period_division', 'consolidation_cat',
                        'report_cat', 'report_detail_cat', 'start_date', 'end_date', 'instant_date',
                        'namespace', 'element', 'context', 'unitref', 'format', 'numeric']

        # DataFrameの変数
        df = pd.DataFrame(columns=dict_columns)

        # 名前空間に対応した日付[開始日、終了日、期末日]のDataFrame
        date_df = context_date_df()

        # 会社情報を格納した辞書を読み込み
        company_datas = company_data_dict()

        # XBRLを取得
        with zipfile.ZipFile(self.xbrl_zip_path, mode='r') as zip_data:

            # サマリ、財務諸表の情報を読み取り
            file_info = list(zip_data.infolist())
            file_info.sort(key=lambda x: x.filename, reverse=True)
            document_list = []
            for info in file_info:
                if re.compile("^.*ixbrl.htm$").match(info.filename):

                    # 過去の書類が存在する場合は除外する
                    document_name = info.filename.split("/")
                    document_name = document_name[len(
                        document_name)-1].split("-")[1]
                    if document_name in document_list:
                        continue
                    else:
                        document_list.append(document_name)

                    # 辞書を格納するリスト
                    list_dict = []

                    # XBRLファイルをスクレイピング
                    soup = bs(zip_data.read(info.filename), 'lxml')

                    # nonfractionタグを抽出
                    nonfraction_tag = soup.findAll('ix:nonfraction')

                    # nonfractionタグの各要素を取得
                    for tag in nonfraction_tag:
                        try:
                            # 無効なタグは対象外
                            if tag.get('xsi:nil') == 'true':
                                raise XbrlValueNoneException("text_value")

                            # 空の辞書を生成********************************************************
                            dict_tag = dict.fromkeys(dict_columns, None)

                            # ********************************************************
                            # 名前空間、属性、コンテキストを取得*************************
                            # ********************************************************
                            tag_namespace = re.sub(":.*$", "", tag.get('name'))
                            tag_element = re.sub("^.*:", "", tag.get('name'))
                            tag_contextref = tag.get('contextref')

                            # *********************************************************
                            # 開始日、終了日、期末日を登録 *******************************
                            # *********************************************************
                            dict_tag['start_date'] = date_df[date_df['id']
                                                             == tag_contextref]['start_date'].iloc[-1]
                            dict_tag['end_date'] = date_df[date_df['id']
                                                           == tag_contextref]['end_date'].iloc[-1]
                            dict_tag['instant_date'] = date_df[date_df['id']
                                                               == tag_contextref]['instant_date'].iloc[-1]

                            # *********************************************************
                            # 会社情報を登録********************************************
                            # *********************************************************
                            dict_tag['reporting_date'] = company_datas['reporting_date']
                            dict_tag['code'] = company_datas['code']
                            dict_tag['period'] = company_datas['period']

                            # *********************************************************
                            # 財表識別区分を登録*****************************************
                            # *********************************************************
                            
                            file_code = str(info.filename).split("/")
                            file_code = file_code[len(file_code)-1].split("-")
                            # 財務諸表と報告書で処理を分岐
                            if re.compile("Attachment").search(info.filename):
                                dict_tag['financial_statement'] = file_code[1]
                                report_cat = file_code[3]
                            else:
                                report_cat = file_code[1]
                            # 期区分、連結・非連結区分が省略されている場合は分岐処理
                            if len(report_cat) == 4:
                                dict_tag['report_cat'] = report_cat[0:4]
                            else:
                                dict_tag['period_division'] = report_cat[0] if re.compile(
                                    "a|s|q").match(report_cat[0]) else None
                                dict_tag['consolidation_cat'] = report_cat[1] if re.compile(
                                    "c|n").match(report_cat[1]) else None
                                dict_tag['report_cat'] = report_cat[2:6]
                                dict_tag['report_detail_cat'] = report_cat[6:8]
                                
                            # *********************************************************
                            # 書類要素名、書類ラベルを登録 *******************************
                            # *********************************************************
                            
                            # JSONファイルを読み込む
                            with open(f"{os.path.dirname(__file__)}\\const\\const.json", mode='r', encoding='utf-8') as const_file:
                                const_dict = json.load(const_file)
                                
                                # 報告書と財務諸表で処理分岐
                                if dict_tag['financial_statement'] is not None:                                    
                                    dict_tag['doc_element'] = const_dict['document_element'][dict_tag['financial_statement']]
                                    dict_tag['doc_label'] = const_dict['document_name'][dict_tag['financial_statement']]
                                
                                elif dict_tag['report_cat'] is not None:                                
                                    dict_tag['doc_label'] = const_dict['report'][dict_tag['report_cat']]    

                            # **********************************************************
                            # 各項目を登録***********************************************
                            # **********************************************************
                            dict_tag['namespace'] = tag_namespace
                            dict_tag['element'] = tag_element
                            dict_tag['context'] = tag_contextref
                            dict_tag['unitref'] = tag.get('unitref')

                            if len(tag.contents) != 0:
                                dict_tag['format'] = re.sub(
                                    "^.*:", "", tag.get('format'))
                                dict_tag['numeric'] = Decimal(
                                    re.sub(r"\D", "", tag.contents[0])) * 10 ** Decimal(tag.get('scale'))

                                # 数値がマイナスの場合
                                if tag.get('sign') == '-':
                                    dict_tag['numeric'] = - \
                                        1 * dict_tag['numeric']

                            # 辞書をリストに追加
                            list_dict.append(dict_tag)
                            # ********************************************************************

                        except TypeError as identifier:
                            pass
                        except XbrlValueNoneException as identifier:
                            pass

                    # DataFrameに変換
                    dict_df = DataFrame(list_dict)
                    if df is None:
                        df = dict_df
                    else:
                        df = pd.concat([df, dict_df])

        return df

    def to_cal_link_df(self) -> DataFrame:
        """各ラベルに対応した計算リンク(DataFrame)を出力します。

        Returns:
            DataFrame: 計算リンク
        """
        
        # XBRLを読み込む
        df = self.xbrl_df
        
        # 空のリスト
        tag_list = []

        # データ構造のみコピー
        add_df = df[:0]
        add_df['from_label'] = None
        add_df['to_label'] = None
        add_df['order'] = None
        add_df['weight'] = None

        # ZIPファイルを展開
        with zipfile.ZipFile(self.xbrl_zip_path, mode='r') as zip_data:
            # ファイルを降順に並び替え
            file_list = list(zip_data.infolist())
            file_list.sort(key=lambda x: x.filename, reverse=True)
            # ファイルリストを展開
            for info in file_list:
                # 計算リンクファイルを抽出
                if re.compile(".*cal.xml").search(info.filename):

                    # 対象ファイル(XML)をスクレイピング
                    soup = bs(zip_data.read(info.filename), 'lxml')

                    # <link:calculationArc> タグを抽出
                    cal_links = soup.find_all(name='link:calculationlink')

                    # タグを登録
                    for link in cal_links:
                        text_block = re.compile("rol_[a-zA-Z0-9]+|Role[a-zA-Z0-9]").search(link.get('xlink:role')).group()
                        text_block = re.sub("rol_|Role", "", text_block)
                        tags = link.find_all('link:calculationarc')
                        
                        for tag in tags:

                            # 空の辞書
                            tag_dict = {}

                            # 要素を登録
                            tag_dict['text_block'] = text_block
                            l_com = "[A-Za-z0-9]+$"
                            tag_dict['from_label'] = re.compile(
                                l_com).search(tag.get('xlink:from')).group()
                            tag_dict['to_label'] = re.compile(
                                l_com).search(tag.get('xlink:to')).group()
                            tag_dict['order'] = tag.get('order')
                            tag_dict['weight'] = tag.get('weight')

                            # リストに追加
                            tag_list.append(tag_dict)

                    break

        # リストからDataFrameに変換
        tag_df = DataFrame(tag_list)

        # 計算リンクをDataFrameに付与
        for name, group in df.groupby(by='report_detail_cat'):
            if name == 'fr':
                group = pd.merge(group, tag_df, how='inner',
                                    left_on=['element','text_block'], right_on=['to_label','text_block'])
                add_df = pd.concat([add_df, group], axis=0)
                
        # Dataframeを並び替え
        add_df = add_df.sort_values(
            by=['report_detail_cat', 'financial_statement'], ascending=[False, True])
        
        # 欠損値をNoneに置換
        add_df = add_df.where(add_df.notnull(), None)
        
        # 重複する行を削除
        add_df = add_df.drop_duplicates()
        
        # 列を抽出する
        add_df = add_df[['reporting_date', 'code', 'text_block', 'namespace', 'element', 'from_label', 'order', 'weight']]
        
        return add_df

    def to_def_link_df(self) -> DataFrame:
        """各ラベルに対応した定義リンク(DataFrame)を出力します。

        Returns:
            DataFrame: 定義リンク
        """
        
        # XBRLを読み込む
        df = self.xbrl_df
        
        # 空のリスト
        sm_tag_list = []
        fr_tag_list = []

        # データ構造のみコピー
        add_df = df[:0]
        add_df = add_df.assign(
            from_label=None, to_label=None, order=None)

        # ZIPファイルを展開
        with zipfile.ZipFile(self.xbrl_zip_path, mode='r') as zip_data:
            # ファイルを降順に並び替え
            file_list = list(zip_data.infolist())
            file_list.sort(key=lambda x: x.filename, reverse=True)
            # ファイルリストを展開
            for info in file_list:
                # 計算リンクファイルを抽出
                if re.compile(".*def.xml").search(info.filename):
                    
                    # 対象ファイル(XML)をスクレイピング
                    soup = bs(zip_data.read(info.filename), 'lxml')
                    
                    # <link:definitionLink> タグを抽出
                    def_links = soup.find_all(name='link:definitionlink')

                    # タグを登録
                    for link in def_links:
                        text_block = re.compile("rol_[a-zA-Z0-9]+|Role[a-zA-Z0-9]").search(link.get('xlink:role')).group()
                        text_block = re.sub("rol_|Role", "", text_block)
                        tags = link.find_all('link:definitionarc')
                    # タグを登録
                        for tag in tags:

                            # 空の辞書
                            tag_dict = {}

                            # 要素を登録
                            tag_dict['text_block'] = text_block
                            l_com = "[A-Za-z0-9]+$"
                            tag_dict['from_label'] = re.compile(
                                l_com).search(tag.get('xlink:from')).group()
                            tag_dict['to_label'] = re.compile(
                                l_com).search(tag.get('xlink:to')).group()
                            tag_dict['order'] = tag.get('order')
                            
                            # リストに追加
                            if re.compile("Summary").search(info.filename):
                                sm_tag_list.append(tag_dict)
                            else:
                                fr_tag_list.append(tag_dict)

                    if re.compile("Attachment").search(info.filename):
                        # 定義リンクをDataFrameに付与
                        for name, group in df.groupby(by='report_detail_cat'):
                            if name == 'fr':
                                tag_df = DataFrame(fr_tag_list)
                                group = pd.merge(group, tag_df, how='inner',
                                                left_on=['element','text_block'], right_on=['to_label','text_block'])
                                add_df = pd.concat([add_df, group], axis=0)

        # Dataframeを並び替え
        add_df = add_df.sort_values(
            by=['report_detail_cat', 'financial_statement'], ascending=[False, True])
        
        # 欠損値をNoneに置換
        add_df = add_df.where(add_df.notnull(), None)
        
        # 重複業を削除
        add_df = add_df.drop_duplicates()
        
        # 列を抽出する
        add_df = add_df[['reporting_date', 'code', 'text_block', 'namespace', 'element', 'from_label', 'order']]
        
        return add_df

    def to_pre_link_df(self) -> DataFrame:
        """各ラベルに対応した表示リンク(DataFrame)を出力します。

        Returns:
            DataFrame: 表示リンク
        """

        # XBRLを読み込む
        df = self.xbrl_df
        
        # 空のリスト
        tag_list = []

        # データ構造のみコピー
        add_df = df[:0]
        add_df['from_label'] = None
        add_df['to_label'] = None
        add_df['order'] = None

        # ZIPファイルを展開
        with zipfile.ZipFile(self.xbrl_zip_path, mode='r') as zip_data:
            # ファイルを降順に並び替え
            file_list = list(zip_data.infolist())
            file_list.sort(key=lambda x: x.filename, reverse=True)
            # ファイルリストを展開
            for info in file_list:
                # 計算リンクファイルを抽出
                if re.compile(".*pre.xml").search(info.filename):
                    
                    # 対象ファイル(XML)をスクレイピング
                    soup = bs(zip_data.read(info.filename), 'lxml')

                    # <link:presentationLink> タグを抽出
                    pre_links = soup.find_all(name='link:presentationlink')

                    # タグを登録
                    for link in pre_links:
                        text_block = re.compile("rol_[a-zA-Z0-9]+|Role[a-zA-Z0-9]").search(link.get('xlink:role')).group()
                        text_block = re.sub("rol_|Role", "", text_block)
                        tags = link.find_all('link:presentationarc')

                        # タグを登録
                        for tag in tags:

                            # 空の辞書
                            tag_dict = {}

                            # 要素を登録
                            tag_dict['text_block'] = text_block
                            l_com = "[A-Za-z0-9]+$"
                            tag_dict['from_label'] = re.compile(
                                l_com).search(tag.get('xlink:from')).group()
                            tag_dict['to_label'] = re.compile(
                                l_com).search(tag.get('xlink:to')).group()
                            tag_dict['order'] = tag.get('order')

                            # リストに追加
                            tag_list.append(tag_dict)

                    break

        # リストからDataFrameに変換
        tag_df = DataFrame(tag_list)

        # 表示リンクをDataFrameに付与
        for name, group in df.groupby(by='report_detail_cat'):
            if name == 'fr':
                group = pd.merge(group, tag_df, how='inner',
                                    left_on=['element','text_block'], right_on=['to_label','text_block'])
                add_df = pd.concat([add_df, group], axis=0)

        # Dataframeを並び替え
        add_df = add_df.sort_values(
            by=['report_detail_cat', 'financial_statement'], ascending=[False, True])
        
        # 欠損値をNoneに置換
        add_df = add_df.where(add_df.notnull(), None)
        
        # 重複行を削除
        add_df = add_df.drop_duplicates()
        
        # 列を抽出する
        add_df = add_df[['reporting_date', 'code', 'text_block', 'namespace', 'element', 'from_label', 'order']]
        
        return add_df

if __name__ == "__main__":
    
    # ************************************************************
    # データベース接続**********************************************
    # ************************************************************
    connection = pymysql.connect(host='localhost',
                                user='root',
                                password='kent6839',
                                database='Stock',
                                cursorclass=pymysql.cursors.DictCursor)

    with connection:
        with connection.cursor() as cursor:
            # レコードを挿入
            sql = 'INSERT IGNORE INTO xbrl_order \
                VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
            cal_sql = 'INSERT IGNORE INTO xbrl_cal_link \
                VALUES(%s,%s,%s,%s,%s,%s,%s,%s)'
            pre_sql = 'INSERT IGNORE INTO xbrl_pre_link \
                VALUES(%s,%s,%s,%s,%s,%s,%s)'
            def_sql = 'INSERT IGNORE INTO xbrl_def_link \
                VALUES(%s,%s,%s,%s,%s,%s,%s)'

            # ************************************************************
            # データ取得***************************************************
            # ************************************************************

            zip_path = Path("D:/ZIP/")
            zip_list = list(zip_path.glob("**/*.zip"))
            for zip_file in zip_list:

                print(zip_file)

                play = XbrlRead(zip_file)
                df = play.add_label_df()
                file_name = os.path.splitext(os.path.basename(zip_file))[0]
                df.to_csv(f'D:/CSV/label/{file_name}.csv')
                cursor.executemany(sql,df.values.tolist())
                cursor.executemany(cal_sql, play.to_cal_link_df().values.tolist())
                cursor.executemany(pre_sql, play.to_pre_link_df().values.tolist())
                cursor.executemany(def_sql, play.to_def_link_df().values.tolist())
                connection.commit()
    