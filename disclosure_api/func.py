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

class NotZipFileExample(Exception):
    def __str__(self) -> str:
        return "ZIPファイルのパスを設定してください。"

class FinanceStatement:
    """TDNETから取得したXBRLの読み込み機能を提供します。
    """
    
    def __init__(self, xbrl_zip_path:str) -> None:
        """TDNETから取得したXBRLの読み込み機能を提供します。

        Args:
            xbrl_zip_path (str): XBRL(ZIP)のパス

        Raises:
            NotZipFileExample: ZIP形式以外のファイルが指定された際に例外が発生します。
        """
        _, ext = os.path.splitext((xbrl_zip_path))
        if ext != ".zip":
            raise NotZipFileExample()
            
        self.xbrl_zip_path = xbrl_zip_path
        self.file_datas = self.__zip_open()
        
    def __zip_open(self) -> OrderedDict:
        """Zip圧縮ファイルからファイルを取り出します。
        
        Returns:
            OrderedDict: 順序付き辞書
        """
        file_datas = OrderedDict()
        try:
            with zipfile.ZipFile(self.xbrl_zip_path,mode='r') as zip_data:
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
        infos = {}
        for (file, data) in self.file_datas.items():
            if re.compile("^.*/Summary/.*-ixbrl.htm$").match(file) is not None:
                file_name = str(file)[str(file).find("tse"):].split("-")
                infos["id"] = file_name[3]
                infos["code"] = file_name[2][:4]
                infos["term"] = const.STATEMENT["term"][file_name[1][-8]]
                infos["consolidated"] = const.STATEMENT["consolidated"][file_name[1][-7]]
                infos["report"] = const.STATEMENT["report"][file_name[1][-6:-2]]
        return infos
    
    def get_documents(self) -> dict:
        files = {}
        for (file, data) in self.file_datas.items():
            if re.compile("^.*/Summary/.*-ixbrl.htm$").match(file) is not None:
                file_name = str(file)[str(file).find("tse"):].split("-")
                files[file_name[1][-6:-2]] = const.STATEMENT["report"][file_name[1][-6:-2]]
            if re.compile("^.*/Attachment/.*-ixbrl.htm$").match(file) is not None:
                m = re.search("[0-9]{7}", file).start()
                file_name = str(file)[m:].split("-")
                files[file_name[1]] = const.STATEMENT["split"][file_name[1]]
        return files
    
    def get_data(self):
        for (file, data) in self.file_datas.items():
            if re.compile("^.*/Attachment/.*-lab.xml$").match(file) is not None:
                local_taxonomy = str(file)
            if re.compile("^.*/Attachment/.*-ixbrl.htm$").match(file) is not None:
                print(file)
                summary = Attachment(data, "doc/taxonomy_tsv/attachment_taxonomy.tsv", local_taxonomy)
                summary.get_labeled_df().to_csv("test.csv")
    
    def __def__(self):
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
                    dict_def["type"] = tag.get("xlink:type")
                    dict_def["from"] = tag.get("xlink:from")
                    dict_def["to"] = tag.get("xlink:to")
                    dict_def["arcrole"] = tag.get("xlink:arcrole")
                    dict_def["order"] = tag.get("order")
                    list_def.append(dict_def)
                
                # dictを格納したリストをDataFrameに変換
                df = DataFrame(list_def)
                df.to_csv("def.csv")
    
    def __cal__(self):
        for (file, data) in self.file_datas.items():
            if re.compile("^.*/Attachment/.*-cal.xml").match(file) is not None:
                soup = bs(data, "lxml")
                # link:definitionArc タグのみ抽出
                
                cal_tag = soup.find_all("link:calculationarc")
                # dictを格納する空のリスト
                list_cal = []
                # dictを作成し、リストに追加する
                for tag in cal_tag:
                    dict_cal = {}
                    dict_cal["type"] = tag.get("xlink:type")
                    dict_cal["from"] = tag.get("xlink:from")
                    dict_cal["to"] = tag.get("xlink:to")
                    dict_cal["arcrole"] = tag.get("xlink:arcrole")
                    dict_cal["order"] = tag.get("order")
                    dict_cal["weight"] = tag.get("weight")
                    list_cal.append(dict_cal)
                
                # dictを格納したリストをDataFrameに変換
                df = DataFrame(list_cal)
                df.to_csv("cal.csv")

    def __pre__(self):
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
                    dict_pre["type"] = tag.get("xlink:type")
                    dict_pre["from"] = tag.get("xlink:from")
                    dict_pre["to"] = tag.get("xlink:to")
                    dict_pre["arcrole"] = tag.get("xlink:arcrole")
                    dict_pre["order"] = tag.get("order")
                    dict_pre["weight"] = tag.get("weight")
                    list_pre.append(dict_pre)
                
                # dictを格納したリストをDataFrameに変換
                df = DataFrame(list_pre)
                df.to_csv("pre.csv")

main = FinanceStatement("D://ZIP/20221026/081220221024548300.zip")
print(main.get_info())
print(main.get_documents())
main.__pre__()