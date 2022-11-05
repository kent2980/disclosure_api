import pandas as pd
from bs4 import BeautifulSoup
from os import path
import glob

path_taxonomy_labels = './taxonomy_global_label.tsv'

# 財務諸表の要素タグからDFを返す関数。
# 5.のサンプルコードからXBRLファイルの読み込みを省略し、引数をタグに変更。
def get_df(arg_tags):
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
        dict_fs['unitRef'] = each_item.get('unitRef')
        # マイナス表記の場合の処理＋円単位への変更
        if each_item.get('sign') == '-' and each_item.get('xsi:nil') != 'true':
            amount = int(each_item.text.replace(',', '')) * -1 * 10 ** int(each_item.get('scale'))
        elif each_item.get('xsi:nil') != 'true':
            amount = int(each_item.text.replace(',', '')) * 10 ** int(each_item.get('scale'))
        else:
            amount = ''
        dict_fs['amount'] = amount
        # 辞書をリストへ格納
        list_fs.append(dict_fs)

    # 辞書を格納したリストをDFに
    df_eachfs = pd.DataFrame(list_fs)

    return df_eachfs


# XBRLのパスから、DFを返す関数本体。
def get_fs(arg_path):
    # fsファイルの読み込み。bs4でパース
    with open(arg_path, encoding='utf-8') as f:
        soup = BeautifulSoup(f.read(), 'lxml')

    # nonNumericタグのみ抽出
    tags_nonnumeric = soup.find_all('ix:nonnumeric')

    # nonnumericの各要素を格納するカラの辞書を作成
    dict_tag = {}
    # nonnumericの内容を辞書型に
    for tag in tags_nonnumeric:
        dict_tag[tag.get('name')] = tag

    # 取得対象となりうる財務諸表の`name`一覧定義
    list_target_fs = [
        # 四半期決算短信[日本基準]
        'jpcrp_cor:QuarterlyConsolidatedBalanceSheetTextBlock',
        'jpcrp_cor:YearToQuarterEndConsolidatedStatementOfIncomeTextBlock',
        'jpcrp_cor:YearToQuarterEndConsolidatedStatementOfComprehensiveIncomeTextBlock',
        'jpcrp_cor:QuarterlyConsolidatedStatementOfCashFlowsTextBlock',
        # 四半期決算短信[IFRS]
        'jpigp_cor:CondensedQuarterlyConsolidatedStatementOfFinancialPositionIFRSTextBlock',
        'jpigp_cor:CondensedYearToQuarterEndConsolidatedStatementOfProfitOrLossIFRSTextBlock',
        'jpigp_cor:CondensedYearToQuarterEndConsolidatedStatementOfComprehensiveIncomeIFRSTextBlock',
        'jpigp_cor:CondensedQuarterlyConsolidatedStatementOfChangesInEquityIFRSTextBlock',
        'jpigp_cor:CondensedQuarterlyConsolidatedStatementOfCashFlowsIFRSTextBlock'
        ]

    # 各財務諸表を入れるカラのDFを作成
    list_fs = []
    # 可能性のある財務諸表区分ごとにループ処理でDF作成
    # dict_tagのキーの中には、財務諸表本表に関係のない注記情報に関するキーもあるため、必要な本表に絞ってループ処理
    for each_target_fs in list_target_fs:
        # ターゲットとなるFS区分のタグを取得
        tag_each_fs = dict_tag.get(each_target_fs)
        # 辞書型の値をgetして、値がなければnoneが返る。noneはfalse扱いのため、これを条件に分岐。
        if tag_each_fs:
            # 財務諸表要素は'ix:nonFraction'に入っているため、このタグを取得
            tag_nonfraction = tag_each_fs.find_all('ix:nonfraction')
            # 財務諸表の各要素をDFに。財務諸表区分とタグを引数にして関数に渡す。
            df_each_fs = get_df(tag_nonfraction)
            df_each_fs['fs_class'] = each_target_fs
            list_fs.append(df_each_fs)

    # タグの中にtarget_FSが含まれない場合（例えば注記だけのixbrlを読み込んだ場合）の分岐
    if list_fs:
        # 各財務諸表の結合
        df_fs = pd.concat(list_fs)

        # 並べ替え
        df_fs = df_fs[['fs_class', 'account_item', 'contextRef', 'format', 'decimals', 'scale', 'unitRef', 'amount']]

    else:
        df_fs = pd.DataFrame(index=[])

    return df_fs

def get_label_local():
    # 引数のdocidからパスを生成
    path_local_label = glob.glob('./sampleXBRLData/081220221031553990/XBRLData/Attachment/*lab.xml')

    # 標準ラベルのみ使用し、独自ラベルを持たない場合があるため、if分岐
    if path_local_label:
        # labファイルの読み込み
        with open(path_local_label[0], encoding='utf-8') as f:
            soup = BeautifulSoup(f.read(), 'lxml')

        # link:labelタグのみ抽出
        link_label = soup.find_all('link:label')

        # ラベル情報用dictを格納するカラのリストを作成
        list_label = []

        # ラベル情報をループ処理で取得
        for each_label in link_label:
            dict_label = {}
            dict_label['id'] = each_label.get('id')
            dict_label['xlink_label'] = each_label.get('xlink:label')
            dict_label['xlink_role'] = each_label.get('xlink:role')
            dict_label['xlink_type'] = each_label.get('xlink:type')
            dict_label['xml_lang'] = each_label.get('xml:lang')
            dict_label['label'] = each_label.text
            list_label.append(dict_label)

        # ラベル情報取得結果をDFに    
        df_label_local = pd.DataFrame(list_label)

    else:
        df_label_local = pd.DataFrame(index=[])

    return df_label_local

def get_labeled_df(arg_fs, arg_label_local):
    
    # 標準ラベルの読み込み
    df_label_global = pd.read_table(path_taxonomy_labels, sep='\t', encoding='utf-8')
    # 標準ラベルの処理
    
    # 標準ラベルデータのうち、必要行に絞る
    df_label_global = df_label_global[df_label_global['xlink_role'] == 'http://www.xbrl.org/2003/role/label']
    
    # 必要列のみに絞る
    df_label_global = df_label_global[['xlink_label', 'label']]
    
    # 'label_'はじまりを削除で統一
    df_label_global['xlink_label'] = df_label_global['xlink_label'].str.replace('label_', '')
    
    # 同一ラベルで異なる表示名が存在する場合、独自の表示名を優先
    df_label_global['temp'] = 0
    
    # 標準ラベルのみ使用し、独自ラベルを持たない場合があるため、if分岐
    if len(arg_label_local) > 0:
        # 独自ラベルの処理
        
        # 独自ラベルデータのうち、必要列に絞る
        df_label_local = arg_label_local[['xlink_label', 'label']].copy()
        
        # ラベルの末尾に'_label.*'があり、FSと結合できないため、これを削除
        df_label_local['xlink_label'] = df_label_local['xlink_label'].str.replace('_label.*$', '').copy()
        
        # ラベルの最初に'jpcrp'で始まるラベルがあり、削除で統一。
        
        # 削除で統一した結果、各社で定義していた汎用的な科目名（「貸借対照表計上額」など）が重複するようになる。後続処理で重複削除。
        df_label_local['xlink_label'] = df_label_local['xlink_label'].str.replace('[a-z]{5}_cor_', '')
        df_label_local['xlink_label'] = df_label_local['xlink_label'].str.replace('[a-z]{3}-[a-z]{8}-[0-9]{5}_', '')
        df_label_local.to_csv('df_label_local.csv')
        
        # ラベルの最初に'label_'で始まるラベルがあり、削除で統一
        
        # 削除で統一した結果、各社で定義していた汎用的な科目名（「貸借対照表計上額」など）が重複するようになる。後続処理で重複削除。
        df_label_local['xlink_label'] = df_label_local['xlink_label'].str.replace('label_', '')
        
        # 同一要素名で異なる表示名が存在する場合、独ラベルを優先
        df_label_local['temp'] = 1

        # label_globalとlabel_localを縦結合
        df_label_merged = pd.concat([df_label_global, df_label_local])
    else:
        df_label_merged = df_label_global
    

    # 同一要素名で異なる表示名が存在する場合、独自ラベルを優先
    grp_df_label_merged = df_label_merged.groupby('xlink_label')
    df_label_merged = df_label_merged.loc[grp_df_label_merged['temp'].idxmax(),:]
    df_label_merged = df_label_merged.drop('temp', axis=1)

    # localラベルで重複してしまう行があるため、ここで重複行を削除
    df_label_merged = df_label_merged.drop_duplicates()

    # 結合用ラベル列作成
    arg_fs['temp_label'] = arg_fs['account_item'].str.replace('[a-z]{5}_cor:', '')
    arg_fs['temp_label'] = arg_fs['temp_label'].str.replace('[a-z]{5}_cor:', '')
    arg_fs['temp_label'] = arg_fs['temp_label'].str.replace('[a-z]{3}-[a-z]{8}-[0-9]{5}:', '')
    arg_fs.to_csv('arg_fs.csv')

    # ラベルの結合
    df_labeled_fs = pd.merge(arg_fs, df_label_merged, left_on='temp_label', right_on='xlink_label', how='left').drop_duplicates()

    return df_labeled_fs

if __name__ == '__main__':
    
    uri = './sampleXBRLData/081220221031553990/XBRLData/Attachment/0500000-qcfs03-tse-qcediffr-72980-2022-09-30-01-2022-11-01-ixbrl.htm'
    
    fs = get_fs(uri)
    
    label_df = get_label_local()
    
    raw_df = get_labeled_df(fs, label_df)
    raw_df.to_csv('raw_df.csv')