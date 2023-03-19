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
import json
import uuid
import jaconv


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


class LinkListEmptyException(MyException):

    def __init__(self, arg: str = "") -> None:
        super().__init__(arg)

    def __str__(self) -> str:
        return f"リンクファイルが読み込めませんでした。\n[{self.arg}]"


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

        # 報告日を取得
        self.publication_date = datetime.strptime(re.compile(
            "[0-9]{8}").search(os.path.dirname(xbrl_zip_path).__str__()).group(), "%Y%m%d")

        # 銘柄コードを取得
        self.code = self.get_company_code()

        # ID(zipファイル名)を取得
        self.id = os.path.splitext(os.path.basename(self.xbrl_zip_path))[0]

        # XBRLからデータフレームを取得
        self.xbrl_df = self.to_dataframe()

        # ラベル(DataFrame)を取得
        self.label_df = self.__get_label_df()

    def __get_label_df(self) -> DataFrame:
        """報告書・財務情報に勘定ラベルを付与します

        Args:
            df (DataFrame): 報告書・財務情報

        Returns:
            DataFrame: 勘定ラベル付き（報告書・財務）情報
        """

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
                local_path = re.sub(r"\\", "/", os.path.join(os.path.abspath(
                    os.path.dirname(__file__)) + '\\doc\\taxonomy' + url_path))
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
                local_path = re.sub(r"\\", "/", os.path.join(os.path.abspath(
                    os.path.dirname(__file__)) + '\\doc\\taxonomy' + url_path))
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

        # 空のリスト
        dict_list = []

        # namespace毎にグループ化
        group_df = self.xbrl_df.groupby('namespace')
        for name, _ in group_df:

            # *************************************************************
            # グローバルラベルリンク ****************************************
            # *************************************************************

            # 名前空間を抽出
            namespace = re.sub("_cor", "", name)

            # タクソノミファイルを展開
            for file in load_global_label_xml():

                # 対象のタクソノミファイルで処理を行う
                if re.compile(f".*{namespace}.*lab.xml$").match(str(file).replace("\\", "/")):

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
                            # namespaceを登録
                            tag_dict['namespace'] = name
                            # elementを抽出
                            tag_dict['element'] = re.sub(
                                "^label_", "", tag.get('xlink:label'))
                            # labelを抽出
                            tag_dict['element_label'] = tag.text
                            # リストに辞書を追加
                            dict_list.append(tag_dict)

            # **************************************************************
            # ローカルラベルリンク *******************************************
            # **************************************************************
            if re.compile("tse-[a-z]{8}-[0-9]{5}").search(name):
                with zipfile.ZipFile(self.xbrl_zip_path, mode='r') as zip_data:

                    file_info = list(zip_data.infolist())
                    file_info.sort(key=lambda x: x.filename, reverse=True)
                    for info in file_info:
                        if re.compile("^.*lab.xml$").match(info.filename):

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
                                # namespaceを登録
                                tag_dict['namespace'] = namespace
                                # elementを抽出
                                element = tag.get('xlink:label')
                                element = re.sub("^label_", "", element)
                                element = re.sub("_label$", "", element)
                                element = re.sub(
                                    f"{namespace}_", "", element)
                                tag_dict['element'] = element
                                # labelを抽出
                                tag_dict['element_label'] = tag.text
                                # リストに辞書を追加
                                dict_list.append(tag_dict)

                            break

        label_df = DataFrame(dict_list)

        return label_df

    def get_company_code(self) -> str:
        """銘柄コードを取得します。

        Returns:
            str: 銘柄コード
        """

        # ZIPを展開
        with zipfile.ZipFile(self.xbrl_zip_path, mode='r') as zip_date:
            for info in zip_date.infolist():
                if re.compile("[0-9]{5}").search(info.filename):
                    # 銘柄コードを取得
                    code = re.compile(
                        "[0-9]{5}").search(info.filename).group()[0:4]

                    return code

    def company_explain_df(self) -> DataFrame:
        """会社の固有情報を出力します。

        Returns:
            dict: 会社固有情報
        """

        # 空のリストを作成
        tag_list = []

        # 空の辞書を作成
        tag_dict = dict.fromkeys(
            ['id' , 'title', 'filing_date', 'publication_date', 'code', 'period', 'period_division', 'period_division_label', 'consolidation_cat',
             'consolidation_cat_label', 'report_cat', 'report_label', 'name'], None)

        # Zipファイルを展開する
        with zipfile.ZipFile(self.xbrl_zip_path, 'r') as zip_data:
            # ファイルを展開する
            for info in zip_data.infolist():
                # 対象のファイルを抽出
                if re.compile("tse-.*ixbrl.htm$|^.*Summary/.*ixbrl.htm$|^.*ixbrl.htm$").search(info.filename):

                    # ***************************************************
                    # ファイル名から取得
                    # ***************************************************
                    file_code = str(info.filename).split("/")
                    file_code = file_code[len(file_code)-1].split("-")
                    # 財務諸表と報告書で処理を分岐
                    if re.compile("Attachment").search(info.filename):
                        report_cat = file_code[3]
                    else:
                        report_cat = file_code[1]

                    # 期区分、連結・非連結区分が省略されている場合は分岐処理
                    if len(report_cat) == 4:
                        tag_dict['report_cat'] = report_cat[0:4]
                    else:
                        tag_dict['period_division'] = report_cat[0] if re.compile(
                            "a|s|q").match(report_cat[0]) else None
                        tag_dict['consolidation_cat'] = report_cat[1] if re.compile(
                            "c|n").match(report_cat[1]) else None
                        tag_dict['report_cat'] = report_cat[2:6]

                    # ***************************************************
                    # スクレイピング
                    # ***************************************************

                    # BeautifulSoupでスクレイピング
                    soup = bs(zip_data.read(info.filename), 'lxml')

                    # 公表日
                    if tag_dict['publication_date'] is None:
                        date_str = re.compile(
                            "[0-9]{8}").search(str(self.xbrl_zip_path)).group()
                        tag_dict['publication_date'] = datetime.strptime(
                            date_str, "%Y%m%d").strftime("%Y-%m-%d")

                    # 報告書タイトル
                    if tag_dict['title'] is None:
                        document_title = soup.find('ix:nonnumeric', attrs={'name': re.compile(
                            '^.*DocumentName')})
                        tag_dict['title'] = jaconv.normalize(document_title.text) if document_title is not None else None

                    # 提出日
                    if tag_dict['filing_date'] is None:
                        filing_date = soup.find('ix:nonnumeric', attrs={'name': re.compile(
                            '^.*filing_date')})
                        if filing_date is not None:
                            filing_date = jaconv.normalize(filing_date.text)
                            tag_dict['filing_date'] = datetime.strptime(filing_date, "%Y年%m月%d日").strftime("%Y-%m-%d")
                    
                    # 会社名
                    if tag_dict['name'] is None:
                        company_name = soup.find('ix:nonnumeric', attrs={'name': [re.compile(
                            '^.*CompanyName'), re.compile('^.*AssetManagerREIT'), re.compile('FilerNameInJapaneseDEI')]})
                        tag_dict['name'] = jaconv.normalize(company_name.text) if company_name is not None else None

                    # 銘柄コード
                    if tag_dict['code'] is None:
                        code = soup.find('ix:nonnumeric', attrs={'name': [re.compile(
                            '^.*SecuritiesCode'), re.compile('^.*SecurityCodeDEI')]})
                        tag_dict['code'] = code.text[0:4] if code is not None else None

                    # 会計期間
                    if tag_dict['period'] is None:
                        period = soup.find('ix:nonfraction', attrs={
                            'name': re.compile('^.*QuarterlyPeriod')})
                        period = period.text if period is not None else None
                        if period is not None:
                            tag_dict['period'] = int(period)
                    if tag_dict['period'] is None:
                        period = soup.find('ix:nonnumeric', attrs={
                            'name': re.compile('^.*CurrentPeriodDEI')})
                        period = period.text if period is not None else None
                        if period is not None:
                            if re.compile('Q[0-9]{1}').search(period):
                                tag_dict['period'] = int(re.compile(
                                    "[0-9]{1}").search(period).group())
                            elif period == 'FY':
                                tag_dict['period'] = 4
                            elif period == 'HY':
                                tag_dict['period'] = 2

        # ***************************************************
        # JSONから取得
        # ***************************************************

        # JSONファイルを読み込む
        with open(f"{os.path.dirname(__file__)}/const/const.json", mode='r', encoding='utf-8') as const_file:
            const_dict = json.load(const_file)

        tag_dict['report_label'] = const_dict['report'][tag_dict['report_cat']]

        if tag_dict['period_division'] is not None:
            tag_dict['period_division_label'] = const_dict['term'][tag_dict['period_division']]
        if tag_dict['consolidation_cat'] is not None:
            tag_dict['consolidation_cat_label'] = const_dict['consolidated'][tag_dict['consolidation_cat']]

        # IDを登録
        tag_dict['id'] = self.id

        # リストに追加
        tag_list.append(tag_dict)

        return DataFrame(tag_list)

    def add_label_df(self) -> DataFrame:
        """勘定科目ラベルを付与した報告書・財務諸表のDataFrameを出力します。

        Returns:
            DataFrame: 勘定科目ラベル付き（報告書・財務）情報
        """

        # XBRLを読み込む
        xbrl_df = self.xbrl_df

        # ラベルを付与
        master_df = pd.merge(xbrl_df, self.label_df, how='left', on=[
                             'namespace', 'element'])

        # 重複業を削除
        master_df = master_df.drop_duplicates().reset_index()

        # カラムを並び替え
        master_df = master_df[['id', 'explain_id', 'publication_date', 'code', 'doc_element', 'doc_label', 'financial_statement', 'report_detail_cat',
                               'start_date', 'end_date', 'instant_date', 'namespace', 'unitref', 'format', 'element', 'element_label', 'context', 'numeric']]

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

                        # ***********************************************
                        # context<会計期間>タグを抽出
                        # ***********************************************
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

        # 辞書のキーを定義する
        dict_columns = ['id', 'explain_id', 'publication_date', 'code', 'doc_element', 'doc_label', 'financial_statement',
                        'report_detail_cat', 'start_date', 'end_date', 'instant_date',
                        'namespace',  'unitref', 'format', 'element', 'context', 'numeric']

        # DataFrameの変数
        df = pd.DataFrame(columns=dict_columns)

        # 名前空間に対応した日付[開始日、終了日、期末日]のDataFrame
        date_df = context_date_df()

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

                            # IDを登録
                            dict_tag['id'] = uuid.uuid4()

                            # explain_IDを登録
                            dict_tag['explain_id'] = self.id

                            # 報告日を登録
                            dict_tag['publication_date'] = self.publication_date

                            # 銘柄コードを登録
                            dict_tag['code'] = self.code

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
                            # 財表識別区分を登録*****************************************
                            # *********************************************************

                            file_code = str(info.filename).split("/")
                            file_code = file_code[len(file_code)-1].split("-")
                            # 財務諸表と報告書で処理を分岐
                            if re.compile("Attachment").search(info.filename):
                                dict_tag['financial_statement'] = file_code[1]
                                report_cat = file_code[3]
                            else:
                                dict_tag['financial_statement'] = "summary"
                                report_cat = file_code[1]

                            # 期区分、連結・非連結区分が省略されている場合は分岐処理
                            if len(report_cat) != 4:
                                dict_tag['report_detail_cat'] = report_cat[6:8]

                            # *********************************************************
                            # 書類要素名、書類ラベルを登録 *******************************
                            # *********************************************************

                            # JSONファイルを読み込む
                            with open(f"{os.path.dirname(__file__)}/const/const.json", mode='r', encoding='utf-8') as const_file:
                                const_dict = json.load(const_file)

                                # 報告書と財務諸表で処理分岐
                                if dict_tag['financial_statement'] == "summary":
                                    dict_tag['doc_label'] = '報告書サマリー'
                                    dict_tag['report_detail_cat'] = 'sm'
                                else:
                                    dict_tag['doc_element'] = const_dict['document_element'][dict_tag['financial_statement']]
                                    dict_tag['doc_label'] = const_dict['document_name'][dict_tag['financial_statement']]

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

    def to_cal_link_df(self) -> tuple:
        """各ラベルに対応した計算リンク(DataFrame)を出力します。

        Returns:
            DataFrame: 計算リンク
        """

        # XBRLを読み込む
        df = self.xbrl_df

        # 空のリスト
        tag_list = []
        name_list = []

        # データ構造のみコピー
        add_df = df[:0]
        add_df['from_element'] = None
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
                        doc_element = re.compile(
                            "rol_[a-zA-Z0-9]+|Role[a-zA-Z0-9]").search(link.get('xlink:role')).group()
                        doc_element = re.sub("rol_|Role", "", doc_element)
                        tags = link.find_all('link:calculationarc')

                        for tag in tags:

                            # 空の辞書
                            tag_dict = {}

                            # *************************
                            # 要素を登録
                            # *************************

                            # 書類種別
                            tag_dict['doc_element'] = doc_element

                            # 名前空間
                            namespace = re.search("#.*_", soup.find(name='link:loc', attrs={
                                                  'xlink:label': tag.get('xlink:to')}).get('xlink:href')).group()
                            tag_dict['namespace'] = re.sub(
                                "_$|#", "", namespace)

                            # 正規表現を定義
                            l_com = f"[a-zA-Z].+_cor_|{tag_dict['namespace']}_|[a-zA-Z].+-[a-zA-Z].+-[0-9-].+_"

                            # 親ラベル
                            tag_dict['from_element'] = re.sub(
                                l_com, "", tag.get('xlink:from'))
                            tag_dict['from_element'] = re.sub(
                                r"_[0-9]$", "", tag_dict['from_element'])

                            # 参照ラベル
                            tag_dict['to_label'] = re.sub(
                                l_com, "", tag.get('xlink:to'))

                            # 順位
                            tag_dict['order'] = tag.get('order')

                            # 重み
                            tag_dict['weight'] = tag.get('weight')

                            # リストに追加
                            tag_list.append(tag_dict)

        # リストからDataFrameに変換
        tag_df = DataFrame(tag_list)

        # 計算リンクをDataFrameに付与
        for name, group in df.groupby(by='report_detail_cat'):
            if name == 'fr':
                # XBRLとリンクファイルを内部結合
                group = pd.merge(group, tag_df, how='inner',
                                 left_on=['element', 'doc_element', 'namespace'], right_on=['to_label', 'doc_element', 'namespace'])
                # マスタフレームに追加
                add_df = pd.concat([add_df, group], axis=0)
                # [report_detail_cat] の値をリストに追加
                name_list.append(name)

        # Dataframeを並び替え
        add_df = add_df.sort_values(
            by=['report_detail_cat', 'financial_statement'], ascending=[False, True])

        # 欠損値をNoneに置換
        add_df = add_df.where(add_df.notnull(), None)

        # ラベルを付与
        add_df = pd.merge(add_df, self.label_df, how='left', left_on=[
                          'from_element'], right_on=['element'])

        # ラベルのカラム名を変更
        add_df = add_df.rename(columns={
                               'element_label': 'from_element_label', 'element_x': 'element', 'namespace_x': 'namespace'})

        # 列を抽出する
        add_df = add_df[['explain_id', 'publication_date', 'code', 'doc_element',
                         'namespace', 'element', 'from_element', 'from_element_label', 'order', 'weight']]

        # リストが空の場合は例外処理
        try:
            if len(add_df) == 0 and 'fr' in name_list:
                raise LinkListEmptyException(self.xbrl_zip_path)
        except LinkListEmptyException as identifier:
            print(identifier)

        # 重複する行を削除
        add_df = add_df.drop_duplicates().reset_index()

        # 一意のIDを付与する
        add_df['id'] = pd.Series([str(uuid.uuid4())
                                  for _ in range(len(add_df))])

        # *****************************
        # 中間テーブルを生成
        # *****************************

        association_df = self.__to_link_association(add_df)

        # カラムの順番を変更
        add_df = add_df[['id', 'publication_date', 'code', 'doc_element',
                         'namespace', 'element', 'from_element', 'from_element_label', 'order', 'weight']]

        return add_df, association_df

    def to_def_link_df(self) -> tuple:
        """各ラベルに対応した定義リンク(DataFrame)を出力します。

        Returns:
            DataFrame: 定義リンク
        """

        # XBRLを読み込む
        df = self.xbrl_df

        # 空のリスト
        sm_tag_list = []
        fr_tag_list = []
        name_list = []

        # データ構造のみコピー
        add_df = df[:0]
        add_df = add_df.assign(
            from_element=None, to_label=None, order=None)

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
                        doc_element = re.compile(
                            "rol_[a-zA-Z0-9]+|Role[a-zA-Z0-9]").search(link.get('xlink:role')).group()
                        doc_element = re.sub("rol_|Role", "", doc_element)
                        tags = link.find_all('link:definitionarc')
                    # タグを登録
                        for tag in tags:

                            # 空の辞書
                            tag_dict = {}

                            # ******************************
                            # 要素を登録
                            # ******************************

                            # 書類品種
                            tag_dict['doc_element'] = doc_element

                            # 名前空間
                            namespace = re.search("#.*_", soup.find(name='link:loc', attrs={
                                                  'xlink:label': tag.get('xlink:to')}).get('xlink:href')).group()
                            tag_dict['namespace'] = re.sub(
                                "_$|#", "", namespace)

                            # 正規表現を定義
                            l_com = f"[a-zA-Z].+_cor_|{tag_dict['namespace']}_|[a-zA-Z].+-[a-zA-Z].+-[0-9-].+_"

                            # 親ラベル
                            tag_dict['from_element'] = re.sub(
                                l_com, "", tag.get('xlink:from'))
                            tag_dict['from_element'] = re.sub(
                                r"_[0-9]$", "", tag_dict['from_element'])

                            # 参照ラベル
                            tag_dict['to_label'] = re.sub(
                                l_com, "", tag.get('xlink:to'))

                            # 順位
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

                                # XBRLとリンクファイルを内部結合
                                group = pd.merge(group, tag_df, how='inner',
                                                 left_on=['element', 'doc_element', 'namespace'], right_on=['to_label', 'doc_element', 'namespace'])
                                # マスタフレームに追加
                                add_df = pd.concat([add_df, group], axis=0)
                                # [report_detail_cat] の値をリストに追加
                                name_list.append(name)
        # Dataframeを並び替え
        add_df = add_df.sort_values(
            by=['report_detail_cat', 'financial_statement'], ascending=[False, True])

        # 欠損値をNoneに置換
        add_df = add_df.where(add_df.notnull(), None)

        # ラベルを付与
        add_df = pd.merge(add_df, self.label_df, how='left', left_on=[
                          'from_element'], right_on=['element'])

        # ラベルのカラム名を変更
        add_df = add_df.rename(columns={
                               'element_label': 'from_element_label', 'element_x': 'element', 'namespace_x': 'namespace'})

        # 列を抽出する
        add_df = add_df[['explain_id', 'publication_date', 'code', 'doc_element',
                         'namespace', 'element', 'from_element', 'from_element_label', 'order']]

        # リストが空の場合は例外処理
        try:
            if len(add_df) == 0 and 'fr' in name_list:
                raise LinkListEmptyException(self.xbrl_zip_path)
        except LinkListEmptyException as identifier:
            print(identifier)

        # 重複業を削除
        add_df = add_df.drop_duplicates().reset_index()

        # 一意のIDを付与する
        add_df['id'] = pd.Series([str(uuid.uuid4())
                                  for _ in range(len(add_df))])

        # *****************************
        # 中間テーブルを生成
        # *****************************

        association_df = self.__to_link_association(add_df)
        
        # カラムの順番を変更
        add_df = add_df[['id', 'publication_date', 'code', 'doc_element',
                         'namespace', 'element', 'from_element', 'from_element_label', 'order']]

        return add_df, association_df

    def to_pre_link_df(self) -> tuple:
        """各ラベルに対応した表示リンク(DataFrame)を出力します。

        Returns:
            DataFrame: 表示リンク
        """

        # XBRLを読み込む
        df = self.xbrl_df

        # 空のリスト
        tag_list = []
        name_list = []

        # データ構造のみコピー
        add_df = df[:0]
        add_df['from_element'] = None
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
                        doc_element = re.compile(
                            "rol_[a-zA-Z0-9]+|Role[a-zA-Z0-9]").search(link.get('xlink:role')).group()
                        doc_element = re.sub("rol_|Role", "", doc_element)
                        tags = link.find_all('link:presentationarc')

                        # タグを登録
                        for tag in tags:

                            # 空の辞書
                            tag_dict = {}

                            # ***********************
                            # 要素を登録
                            # ***********************

                            # 書類品種
                            tag_dict['doc_element'] = doc_element

                            # 名前空間
                            namespace = re.search("#.*_", soup.find(name='link:loc', attrs={
                                                  'xlink:label': tag.get('xlink:to')}).get('xlink:href')).group()
                            tag_dict['namespace'] = re.sub(
                                "_$|#", "", namespace)

                            # 正規表現を定義
                            l_com = f"[a-zA-Z].+_cor_|{tag_dict['namespace']}_|[a-zA-Z].+-[a-zA-Z].+-[0-9-].+_"

                            # 親ラベル
                            tag_dict['from_element'] = re.sub(
                                l_com, "", tag.get('xlink:from'))
                            tag_dict['from_element'] = re.sub(
                                r"_[0-9]$", "", tag_dict['from_element'])

                            # 参照ラベル
                            tag_dict['to_label'] = re.sub(
                                l_com, "", tag.get('xlink:to'))

                            # 順位
                            tag_dict['order'] = tag.get('order')

                            # リストに追加
                            tag_list.append(tag_dict)

                    break

        # リストからDataFrameに変換
        tag_df = DataFrame(tag_list)

        # 表示リンクをDataFrameに付与
        for name, group in df.groupby(by='report_detail_cat'):
            if name == 'fr':

                # XBRLとリンクファイルを内部結合
                group = pd.merge(group, tag_df, how='inner',
                                 left_on=['element', 'doc_element', 'namespace'], right_on=['to_label', 'doc_element', 'namespace'])

                # マスタフレームに追加
                add_df = pd.concat([add_df, group], axis=0)

                # [report_detail_cat] の値をリストに追加
                name_list.append(name)

        # Dataframeを並び替え
        add_df = add_df.sort_values(
            by=['report_detail_cat', 'financial_statement'], ascending=[False, True])

        # 欠損値をNoneに置換
        add_df = add_df.where(add_df.notnull(), None)

        # ラベルを付与
        add_df = pd.merge(add_df, self.label_df, how='left', left_on=[
                          'from_element'], right_on=['element'])

        # ラベルのカラム名を変更
        add_df = add_df.rename(columns={
                               'element_label': 'from_element_label', 'element_x': 'element', 'namespace_x': 'namespace'})

        # 列を抽出する
        add_df = add_df[['explain_id', 'publication_date', 'code', 'doc_element',
                         'namespace', 'element', 'from_element', 'from_element_label', 'order']]

        # リストが空の場合は例外処理
        try:
            if len(add_df) == 0 and 'fr' in name_list:
                raise LinkListEmptyException(self.xbrl_zip_path)
        except LinkListEmptyException as identifier:
            print(identifier)

        # 重複業を削除
        add_df = add_df.drop_duplicates().reset_index()

        # 一意のIDを付与する
        add_df['id'] = pd.Series([str(uuid.uuid4())
                                  for _ in range(len(add_df))])

        # *****************************
        # 中間テーブルを生成
        # *****************************

        association_df = self.__to_link_association(add_df)

        # カラムの順番を変更
        add_df = add_df[['id', 'publication_date', 'code', 'doc_element',
                         'namespace', 'element', 'from_element', 'from_element_label', 'order']]
        return add_df, association_df

    def __to_link_association(self, link_df: DataFrame) -> DataFrame:

        # id列の名称変更
        xbrl_df = self.xbrl_df.rename(columns={'id': 'item_id'})
        link_df = link_df.rename(columns={'id': 'link_id'})

        # XBRL(DF)とリンク(DF)をマージ
        association_df = xbrl_df.merge(link_df, how='right', on=[
                                       'explain_id', 'doc_element', 'namespace', 'element'])

        # カラムを並び替え
        association_df = association_df[['item_id', 'link_id']]

        return association_df
