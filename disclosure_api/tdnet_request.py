import urllib.request, urllib.error
from bs4 import BeautifulSoup
from datetime import date
import time
import requests
import os
from tqdm import tqdm
from os.path import dirname, abspath
import sys

# 絶対パスをsys.pathに登録する
parent_dir = dirname(abspath(__file__))
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

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
        
    def getXBRL_link(self, dte:date=None) -> list:
        """TDNETからXBRLをダウンロードします。

        Args:
            dte (date, optional): 日付

        Raises:
            DateIsNoneException: 日付未設定エラー

        Returns:
            list: ファイルの保存パスリスト
        """
        
        # 日付が未設定の場合、例外が発生
        if date is None:
            raise DateIsNoneException()
        
        # ファイルのリンクパスリスト
        file_list = []
        
        # 保存ファイルのリスト
        save_f_list = []
        
        # 保存フォルダ
        saveDir = f'{self.output_dir}/{dte.strftime("%Y%m%d")}'
        
        # ページの存在可否フラグ
        flag = True
        
        # ページ番号
        p = 0
        
        # ファイルリストカウント
        n = 1
        
        # 新規ダウンロードカウンタ
        new_count = 0
        
        # Trueの間は処理を繰り返す
        while flag == True:
            
            # ページ番号をカウントアップ
            p += 1
            
            # 指定された日付のTDNET:URLを生成する
            url =f'https://www.release.tdnet.info/inbs/I_list_{p:03}_{dte.strftime("%Y%m%d")}.html'
            
            # 有効なURLか判定する
            flag = self.__link_check(url)
            
            # 有効なURLだった場合
            if flag == True:
                
                # URLからHTMLを読み込む
                url_txt = requests.get(url).text
                
                # HTMLをスクレイピング
                soup = BeautifulSoup(url_txt, 'html.parser')
                
                # ファイル名をリストに追加
                for el in soup.find_all('div', class_='xbrl-mask'):
                    file_list.append(el.a["href"])
                
        # ファイル名がリストに存在する場合
        if len(file_list) > 0:
            
            # ダウンロード開始のメッセージを表示
            print("\n*********************************************************")
            print(f"     {dte.strftime('%Y年%m月%d日')}公表分をダウンロード...")
            print("*********************************************************\n")
            
            # ダウンロード一覧のプログレスバーを生成
            bar = tqdm(total=len(file_list))
            
            # ******************************************
            # ファイルのダウンロード処理 *****************
            # ******************************************
            
            for file_name in file_list:
                
                # ファイルのダウンロード中カウント遷移
                bar.set_description(f'  {n}/{len(file_list)} 件 処理中・・・')
                
                # 保存先フォルダが存在しない場合は新規作成する
                if not os.path.exists(saveDir):
                    os.makedirs(saveDir)
                    
                # ローカルにファイルが存在しない場合ファイルをダウンロード
                if not self.__isfile(file_name, saveDir):
                    
                    # ダウンロードリンクを生成
                    xbrl_link = f'https://www.release.tdnet.info/inbs/{file_name}'
                    
                    # ローカルの保存パス
                    local_file_path = f'{saveDir}/{file_name}'
                    
                    # ローカルにダウンロード
                    urllib.request.urlretrieve(xbrl_link, local_file_path)
                    
                    # 新規ダウンロードカウンタのアップデート
                    new_count += 1
                    
                    # 保存したファイルをリストに追加
                    save_f_list.append(file_name)
                
                # プログレスバーの表示をアップデート
                bar.update(1)
                
                # 1秒待機
                time.sleep(0.1)
                
                # リストのファイル数をカウント
                n += 1
            
            # プログレスバーの処理をクローズ
            bar.close()           

            # ファイルダウンロード完了後のメッセージ
            if len(save_f_list) > 0:
                print(f"\n     適時開示情報を{new_count}件 新規ダウンロードしました。")
            else:
                print(f"\n     適時開示情報の新規ダウンロードはありません。")
            print(f"     本日発表された適時開示情報は{len(file_list)}件です。\n")
            
        # ファイルリストが空の場合
        else:            
            # メッセージ
            print("*********************************************************\n")
            print(f"     {dte.strftime('%Y年%m月%d日')}は適時開示の発表がありません。\n")
        
        return save_f_list
    
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
