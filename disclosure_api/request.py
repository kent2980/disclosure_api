import urllib.request, urllib.error
from bs4 import BeautifulSoup
from datetime import date
import time
import requests
import os
from tqdm import tqdm
from util._function import date_range

class OutputPathIsNoneException(Exception):
    """出力先パスが"None"の際に設定する例外

    Args:
        Exception (_type_): 基底例外クラス
    """
    def __str__(self) -> str:
        return "出力先パスを設定してください。"
    
class DateIsNoneException(Exception):
    """日付が"None"の際に設定する例外

    Args:
        Exception (_type_): 基底例外クラス
    """
    def __str__(self) -> str:
        return "日付を設定してください。"

class TdnetRequest:
    """TDNETからXBRLをダウンロードする機能を提供します。
    """

    def __init__(self, output_dir:str=None) -> None:
        """TDNETからXBRLをダウンロードする機能を提供します。

        Args:
            output_dir (str, optional): 出力先パス

        Raises:
            OutputPathIsNoneException: 出力先未設定エラー
        """
        if output_dir is None:
            raise OutputPathIsNoneException()
        self.output_dir = output_dir
    
    def __isfile(self, filename:str, saveDir) -> bool:
        """出力先ファイルの存在を判定

        Args:
            filename (str): ファイル名
            saveDir (_type_): フォルダパス

        Returns:
            bool: 判定フラグ
        """
        path = f"{saveDir}/{filename}"
        flag = os.path.isfile(path)
        return flag
    
    def __link_check(self, url: str) -> bool:
        """リンクが有効であるか判定する

        Args:
            url (str): URL

        Returns:
            bool: 判定フラグ
        """
        try:
            f = urllib.request.urlopen(url)
            return True
        except:
            return False
        finally:
            if "f" in locals():
                f.close()
        
    def getXBRL_link(self, dte:date=None) -> int:
        """TDNETからXBRLをダウンロードします。

        Args:
            dte (date, optional): 日付

        Raises:
            DateIsNoneException: 日付未設定エラー

        Returns:
            int: 保存件数
        """
        if date is None:
            raise DateIsNoneException()
        saveDir = f'{self.output_dir}/{dte.strftime("%Y%m%d")}'
        flag = True
        # ページ番号
        p = 0
        # 件数
        n = 0
        while flag == True:
            p += 1
            url =f'https://www.release.tdnet.info/inbs/I_list_{p:03}_{dte.strftime("%Y%m%d")}.html'
            flag = self.__link_check(url)
            if flag == True:
                
                url_txt = requests.get(url).text
                soup = BeautifulSoup(url_txt, 'html.parser')
                file_list = []
                for el in soup.find_all('div', class_='xbrl-mask'):
                    file_list.append(el.a["href"])
                if len(file_list) > 0:
                    if p == 1:
                        print("*********************************************************")
                        print(f"{dte.strftime('%Y年%m月%d日')}公表分をダウンロード...\n")
                    bar = tqdm(total=len(file_list))
                    bar.set_description(f'{p} page reading')
                    for file_ in file_list:
                        if not os.path.exists(saveDir):
                            os.makedirs(saveDir)
                        if not self.__isfile(file_, saveDir):
                            xbrl_link = f'https://www.release.tdnet.info/inbs/{file_}'
                            urllib.request.urlretrieve(xbrl_link, f'{saveDir}/{file_}')
                        bar.update(1)
                        time.sleep(0.1)
                        n += 1
                    bar.close()           
        if n == 0:
            print("*********************************************************\n")
            print(f"{dte.strftime('%Y年%m月%d日')}は適時開示の発表がありません。\n")
        else:
            print(f"\n{n}件の適時開示情報をダウンロードしました。\n")
        return n
    
    def getXBRL_link_daterange(self, start:date=None, end:date=None):
        """TDNETから対象期間に公表されたXBRLをダウンロードします。

        Args:
            start (date, optional): 開始日
            end (date, optional): 終了日

        Raises:
            DateIsNoneException: 日付未設定エラー
        """
        if start is None or end is None:
            raise DateIsNoneException()
        for dte in date_range(start, end):
            self.getXBRL_link(dte)
