import abc
from pandas import DataFrame
import pandas as pd
from decimal import Decimal

class IXbrlCollection(object, metaclass=abc.ABCMeta):
    """XBRLをDataFrameで出力する際の基底クラス

    Args:
        metaclass (_type_, optional): メタクラス
    """

    def __init__(self, arg_data, path_taxonomy_labels) -> None:
        self.data = arg_data
        self.taxonomy_labels = path_taxonomy_labels

    @classmethod
    @abc.abstractmethod
    def get_df(self, arg_tags) -> DataFrame:
        """ XBRLタグ [ix:nonfraction] ResultSetからDFを返す関数。

        Args:
            arg_tags (_type_): XBRLタグ [ix:nonfraction] のResultSet

        Raises:
            NotImplementedError: メソッド未定義例外

        Returns:
            DataFrame: XBRLタグ [ix:nonfraction] DataFrame
        """
        raise NotImplementedError()

    @classmethod
    @abc.abstractmethod
    def get_fs(self, arg_data) -> DataFrame:
        """XBRLのパスから、DFを返す関数本体。

        Args:
            arg_data (_type_): XBRL.htmファイルの実データ。

        Raises:
            NotImplementedError: メソッド未定義例外

        Returns:
            DataFrame: XBRLデータ
        """
        raise NotImplementedError()

    @classmethod
    @abc.abstractmethod
    def get_labeled_df(self) -> DataFrame:
        """財務諸表の各ラベルを付与したDataFrameを返します。

        Raises:
            NotImplementedError: メソッド未定義例外

        Returns:
            DataFrame: ラベルを付与したXBRLデータ
        """
        raise NotImplementedError()

class ITdnetCollection(IXbrlCollection):
    """TDNETより取得したXBRLからDataFrameを取得する際の基底クラス

    Args:
        IXbrlCollection (_type_): XBRL基底クラス
    """
    
    def __init__(self, arg_data, path_taxonomy_labels) -> None:
        super().__init__(arg_data, path_taxonomy_labels)
        
    def get_df(self, arg_tags) -> DataFrame:
        # 各fsの各要素を格納した辞書を入れるカラのリスト作成
        list_fs = []
        # 各fsの各要素を辞書に格納
        for each_item in arg_tags:
            dict_fs = {}
            dict_fs['account_item'] = each_item.get('name')
            dict_fs['contextRef'] = each_item.get('contextref')
            dict_fs['format'] = each_item.get('format')
            dict_fs['decimals'] = each_item.get('decimals')
            dict_fs['scale'] = each_item.get('scale')
            dict_fs['unitref'] = each_item.get('unitref')
            # マイナス表記の場合の処理＋円単位への変更
            if each_item.get('sign') == '-' and each_item.get('xsi:nil') != 'true':
                try:
                    amount = int(each_item.text.replace(',', '')) * -1 * 10 ** int(each_item.get('scale'))
                except ValueError:
                    amount = Decimal(each_item.text.replace(',', '')) * -1 * 10 ** Decimal(each_item.get('scale'))
            elif each_item.get('xsi:nil') != 'true':
                try:
                    amount = int(each_item.text.replace(',', '')) * 10 ** int(each_item.get('scale'))
                except:
                    amount = Decimal(each_item.text.replace(',', '')) * 10 ** Decimal(each_item.get('scale'))
            else:
                amount = ''
            dict_fs['amount'] = amount
            # 辞書をリストへ格納
            list_fs.append(dict_fs)

        # 辞書を格納したリストをDFに
        df_eachfs = pd.DataFrame(list_fs)

        return df_eachfs