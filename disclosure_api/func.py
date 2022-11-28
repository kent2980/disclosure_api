import re
import zipfile
from collections import OrderedDict
import traceback
import os
import const
from xbrl.summary import Summary
from xbrl.attachment import Attachment
from bs4 import BeautifulSoup as bs
from pandas import DataFrame
import pandas as pd


class NotZipFileException(Exception):
    def __str__(self) -> str:
        return "ZIPファイルのパスを設定してください。"


class NoneDocumentCodeException(Exception):
    def __init__(self, documents: dict):
        self.documents = documents

    def __str__(self) -> str:
        return f"{self.documents} から検索対象の書類キーを選択してください。"

class NotSummaryInfo(Exception):
    def __str__(self) -> str:
        return "サマリ情報が存在しません。この操作は無効です。"

class FinanceStatement:

    def __init__(self, xbrl_zip_path: str) -> None:
        """TDNETから取得したXBRLの読み込み機能を提供します。

        Args:
            xbrl_zip_path (str): XBRL(ZIP)のパス

        Raises:
            NotZipFileException: ZIP形式以外のファイルが指定された際に例外が発生します。
        """
        _, ext = os.path.splitext((xbrl_zip_path))
        if ext != ".zip":
            raise NotZipFileException()

        self.xbrl_zip_path = xbrl_zip_path
        self.file_datas = self.__zip_open()

    def __zip_open(self) -> OrderedDict:
        """Zip圧縮ファイルからファイルを取り出します。

        Returns:
            OrderedDict: 順序付き辞書
        """
        file_datas = OrderedDict()
        try:
            with zipfile.ZipFile(self.xbrl_zip_path, mode='r') as zip_data:
                # ファイルリスト取得
                infos = zip_data.infolist()
                # 拡張子でファイルを選択してみる
                # re_match = re.compile(r'^.*?ixbrl.htm$').match

                for info in infos:

                    # zipからファイルを読み込む
                    file_data = zip_data.read(info.filename)

                    # ファイルパスをキーにして辞書に入れる
                    file_datas[info.filename] = file_data

        except zipfile.BadZipFile:
            print(traceback.format_exc())

        return file_datas

    def get_info(self) -> dict:
        """XBRLの基本情報を参照します。

        Returns:
            dict:基本情報 
        """
        infos = {}
        for (file, _) in self.file_datas.items():
            if re.compile("^.*/Summary/.*-ixbrl.htm$").match(file) is not None:
                file_name = str(file)[str(file).find("tse"):].split("-")
                infos["id"] = file_name[3]
                infos["code"] = file_name[2][:4]
                infos["term"] = const.STATEMENT["term"][file_name[1][-8]]
                infos["consolidated"] = const.STATEMENT["consolidated"][file_name[1][-7]]
                infos["report"] = const.STATEMENT["report"][file_name[1][-6:-2]]
        return infos

    def get_documents(self) -> dict:
        """書類情報一覧を参照します。

        Returns:
            dict: 書類一覧
        """
        files = {}
        for (file, _) in self.file_datas.items():
            if re.compile("^.*/Summary/.*-ixbrl.htm$").match(file) is not None:
                file_name = str(file)[str(file).find("tse"):].split("-")
                files[file_name[1][-6:-2]
                      ] = const.STATEMENT["report"][file_name[1][-6:-2]]
            if re.compile("^.*/Attachment/.*-ixbrl.htm$").match(file) is not None:
                m = re.search("[0-9]{7}", file).start()
                file_name = str(file)[m:].split("-")
                files[file_name[1]] = const.STATEMENT["split"][file_name[1]]
        return files

    def __get_AttachmentDF(self, document_key: str = None) -> DataFrame:
        """財務諸表をDataFrameとして出力します。

        Args:
            document_key (str, optional): 書類コード

        Raises:
            NoneDocumentCodeException: 書類コード未設定エラー

        Returns:
            DataFrame: 財務諸表データ

        Example:
            >>> for (key, value) in get_documents.items():
                df = get_attachment_df(key)
        """

        df = None
        local_taxonomy_data = None

        # 引数が未設定の場合例外を投げる
        if document_key is None:
            raise NoneDocumentCodeException(self.get_documents())

        # ローカルラベルを参照
        for (file, data) in self.file_datas.items():
            if re.compile("^.*/Attachment/.*-lab.xml$").match(file) is not None:
                local_taxonomy_data = data

        # ファイルをDataFrameに変換
        for (file, data) in self.file_datas.items():
            
            # 正規表現と比較してXBRLファイルを抽出
            if re.compile("^.*/Attachment/.*-ixbrl.htm$").match(file) is not None:
                
                # ファイル名を分割
                m = re.search("[0-9]{7}", file).start()
                file_name = str(file)[m:].split("-")
                
                # ドキュメントコードと一致するファイルを抽出
                if file_name[1] == document_key:
                    
                    # DataFrameに変換
                    attachment = Attachment(
                        data, "doc/taxonomy_tsv/attachment_taxonomy.tsv", local_taxonomy_data)
                    df = attachment.get_labeled_df()
                    
                    # 各リンクファイルを結合
                    df = pd.merge(df, self.__cal_xml(
                    ), left_on="temp_label", right_on="cal_to", how="left")
                    df = pd.merge(df, self.__def_xml(
                    ), left_on="temp_label", right_on="def_to", how="inner")
                    df = pd.merge(df, self.__pre_xml(
                    ), left_on="temp_label", right_on="pre_to", how="left")

        return df

    def __get_SummaryInfo(self) -> DataFrame:
        df = None
        for (file, data) in self.file_datas.items():
            # 正規表現と比較してXBRLファイルを抽出
            if re.compile("^.*/Summary/.*-ixbrl.htm$").match(file) is not None:
                summary = Summary(
                    data, "doc/taxonomy_tsv/summary_taxonomy.tsv")
                df = summary.get_labeled_df()
        if df is None:
            raise NotSummaryInfo()
        else:
            return df
    
    def get_dataframe(self, document_key:str=None) -> DataFrame:
              
        df = self.__get_AttachmentDF(document_key)
        if df is None:
            df = self.__get_SummaryInfo()
        return df

    def __def_xml(self) -> DataFrame:
        """定義リンクを出力します。

        Returns:
            DataFrame: 定義リンク
        """
        df = None
        for (file, data) in self.file_datas.items():
            if re.compile("^.*/Attachment/.*-def.xml").match(file) is not None:
                soup = bs(data, "lxml")
                # link:definitionArc タグのみ抽出

                definitionTag = soup.find_all("link:definitionarc")
                # dictを格納する空のリスト
                list_def = []
                # dictを作成し、リストに追加する
                for tag in definitionTag:
                    dict_def = {}
                    dict_def["def_type"] = tag.get("xlink:type")
                    dict_def["def_from"] = tag.get("xlink:from")
                    dict_def["def_to"] = tag.get("xlink:to")
                    dict_def["def_arcrole"] = tag.get("xlink:arcrole")
                    dict_def["def_order"] = tag.get("order")
                    list_def.append(dict_def)

                # dictを格納したリストをDataFrameに変換
                df = DataFrame(list_def)
        return df

    def __cal_xml(self) -> DataFrame:
        """計算リンクを出力します。

        Returns:
            DataFrame: 計算リンク
        """
        df = None
        for (file, data) in self.file_datas.items():
            if re.compile("^.*/Attachment/.*-cal.xml").match(file) is not None:
                soup = bs(data, "lxml")
                # link:definitionArc タグのみ抽出

                cal_tag = soup.find_all("link:calculationarc")
                # dictを格納する空のリスト
                list_cal = []
                # dictを作成し、リストに追加する
                for tag in cal_tag:
                    try:
                        dict_cal = {}
                        dict_cal["cal_type"] = tag.get("xlink:type")
                        dict_cal["cal_from"] = tag.get("xlink:from")
                        dict_cal["cal_to"] = tag.get("xlink:to")
                        dict_cal["cal_arcrole"] = tag.get("xlink:arcrole")
                        dict_cal["cal_order"] = tag.get("order")
                        dict_cal["cal_weight"] = tag.get("weight")
                        list_cal.append(dict_cal)
                    except IndexError as identifier:
                        pass

                # dictを格納したリストをDataFrameに変換
                df = DataFrame(list_cal)
        return df

    def __pre_xml(self) -> DataFrame:
        """表示リンクを出力します。

        Returns:
            DataFrame: 表示リンク
        """
        df = None
        for (file, data) in self.file_datas.items():
            if re.compile("^.*/Attachment/.*-pre.xml").match(file) is not None:
                soup = bs(data, "lxml")
                # link:definitionArc タグのみ抽出

                pre_tag = soup.find_all("link:presentationarc")
                # dictを格納する空のリスト
                list_pre = []
                # dictを作成し、リストに追加する
                for tag in pre_tag:
                    dict_pre = {}
                    dict_pre["pre_type"] = tag.get("xlink:type")
                    dict_pre["pre_from"] = tag.get("xlink:from")
                    dict_pre["pre_to"] = tag.get("xlink:to")
                    dict_pre["pre_arcrole"] = tag.get("xlink:arcrole")
                    dict_pre["pre_order"] = tag.get("order")
                    list_pre.append(dict_pre)

                # dictを格納したリストをDataFrameに変換
                df = DataFrame(list_pre)
        return df

