import re
import zipfile
from collections import OrderedDict
import traceback
import os
from bs4 import BeautifulSoup as bs
from pandas import DataFrame
import pandas as pd
from os.path import dirname, abspath
import sys

# 絶対パスをsys.pathに登録する
parent_dir = dirname(abspath(__file__))
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

from xbrl.summary import Summary
from xbrl.attachment import Attachment
from const.const import STATEMENT
class __NotZipFileException(Exception):
    
    def __str__(self) -> str:
        return "ZIPファイルのパスを設定してください。"


class __NoneDocumentCodeException(Exception):
    def __init__(self, documents: dict):
        self.documents = documents

    def __str__(self) -> str:
        return f"{self.documents} から検索対象の書類キーを選択してください。"


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
            raise __NotZipFileException()

        self.xbrl_zip_path = xbrl_zip_path
        self.file_datas = self.__zip_open()
        self.taxonomy_dir = f"{os.path.abspath(os.path.dirname(__file__))}/doc/taxonomy_tsv/"

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
                infos["term"] = STATEMENT["term"][file_name[1][-8]]
                infos["consolidated"] = STATEMENT["consolidated"][file_name[1][-7]]
                infos["report"] = STATEMENT["report"][file_name[1][-6:-2]]
        return infos

    def get_documents(self) -> dict:
        """書類情報一覧を参照します。

        Returns:
            dict: 書類一覧
        """
        files = {}
        for (file, _) in self.file_datas.items():
            if re.compile("^.*/Attachment/.*-ixbrl.htm$").match(file) is not None:
                m = re.search("[0-9]{7}", file).start()
                file_name = str(file)[m:].split("-")
                files[file_name[1]] = STATEMENT["split"][file_name[1]]
            elif re.compile("^.*/Summary/.*-ixbrl.htm$").match(file) is not None:
                file_ = os.path.basename(file)
                file_name = str(file_).split("-")
                files[file_name[1][-6:-2]
                      ] = STATEMENT["report"][file_name[1][-6:-2]]
            elif re.compile("^.*-ixbrl.htm$").match(file) is not None:
                file_ = os.path.basename(file)
                file_name = str(file_).split("-")
                files[file_name[1]
                      ] = STATEMENT["report"][file_name[1]]
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

        # タクソノミの参照パス
        taxonomy_file_name = "attachment_taxonomy.tsv"
        taxonomy_path = os.path.join(self.taxonomy_dir, taxonomy_file_name)
        
        # 引数が未設定の場合例外を投げる
        if document_key is None:
            raise __NoneDocumentCodeException(self.get_documents())

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
                        data, taxonomy_path, local_taxonomy_data)
                    df = attachment.get_labeled_df()
                    # 各リンクファイルを結合
                    df = pd.merge(df, self.__cal_xml(
                    ), left_on="xlink_label", right_on="cal_to", how="left")
                    df = pd.merge(df, self.__def_xml(
                    ), left_on="xlink_label", right_on="def_to", how="left")
                    df = pd.merge(df, self.__pre_xml(
                    ), left_on="xlink_label", right_on="pre_to", how="left")

        return df

    def __get_SummaryInfo(self, document_key:str=None) -> DataFrame:
        
        # タクソノミの参照パス
        taxonomy_file_name = "summary_taxonomy.tsv"
        taxonomy_path = os.path.join(self.taxonomy_dir, taxonomy_file_name)
        
        # 引数が未設定の場合例外を投げる
        if document_key is None:
            raise __NoneDocumentCodeException(self.get_documents())
        
        df = None
        
        for (file, data) in self.file_datas.items():
            # ファイル名をアンダーバーで分割
            file_ = os.path.basename(file)
            file_name = str(file_).split("-")
            # 正規表現と比較してXBRLファイルを抽出
            if re.compile("^.*/Summary/.*-ixbrl.htm$").match(file) is not None:
                if file_name[1][-6:-2] == document_key:
                    summary = Summary(
                        data, taxonomy_path)
                    df = summary.get_labeled_df()
            elif re.compile("^.*-ixbrl.htm$").match(file) is not None:
                if file_name[1] == document_key:
                    summary = Summary(
                        data, taxonomy_path)
                    df = summary.get_labeled_df()
        if df is None:
            raise __NoneDocumentCodeException(self.get_documents())
        else:
            return df

    def get_dataframe(self, document_key: str = None) -> DataFrame:

        df = self.__get_AttachmentDF(document_key)
        if df is None:
            df = self.__get_SummaryInfo(document_key)
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
                    dict_def["def_from"] = tag.get("xlink:from")
                    dict_def["def_to"] = tag.get("xlink:to")
                    dict_def["def_order"] = tag.get("order")
                    list_def.append(dict_def)

                # dictを格納したリストをDataFrameに変換
                df = DataFrame(list_def)
                # 末尾ラベルを削除
                df["def_to"] = df["def_to"].str.replace("_.*$","")
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
                        dict_cal["cal_from"] = tag.get("xlink:from")
                        dict_cal["cal_to"] = tag.get("xlink:to")
                        dict_cal["cal_order"] = tag.get("order")
                        dict_cal["cal_weight"] = tag.get("weight")
                        list_cal.append(dict_cal)
                    except IndexError as identifier:
                        pass

                # dictを格納したリストをDataFrameに変換
                df = DataFrame(list_cal)
                # 末尾ラベルを削除
                df["cal_to"] = df["cal_to"].str.replace("_.*$","")
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
                    dict_pre["pre_from"] = tag.get("xlink:from")
                    dict_pre["pre_to"] = tag.get("xlink:to")
                    dict_pre["pre_order"] = tag.get("order")
                    list_pre.append(dict_pre)

                # dictを格納したリストをDataFrameに変換
                df = DataFrame(list_pre)
                # 末尾ラベルを削除
                df["pre_to"] = df["pre_to"].str.replace("_.*$","")
        return df

list = os.listdir("D:/ZIP")
path = "D:/ZIP/20221130/081220221130572907.zip"
fi = FinanceStatement(path)
doc = fi.get_documents()
for (key, value) in doc.items():
    print(key)
    df = fi.get_dataframe(key)
    df.to_csv(f"{key}.csv")