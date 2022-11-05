import re
import pandas as pd
from bs4 import BeautifulSoup
import glob
import os

def get_global_label(arg_path):
    '''
    金融庁が公開しているEDINETタクソノミから、XBRLタグ名と日本語ラベルの対応関係を示す、日本語ラベルマスタを作成する関数
    引数(arg_path)：*_lab.xmlのファイルパス
    '''
    # labファイルの読み込み
    with open(arg_path, encoding='utf-8') as f:
        soup = BeautifulSoup(f.read(), 'lxml')

    # link:locタグのみ抽出
    link_loc = soup.find_all('link:loc')

    # link:locタグの取得結果をdictにし、dictを格納するカラのリストを作成
    list_locator = []

    # link:locタグの情報をループ処理で取得
    for each_loc in link_loc:
        dict_locator = {}
        shema = each_loc.get('xlink:href').split(sep='#')[0]
        dict_locator['xmlns_jpcrp_ymd'] = re.findall(r'\d{4}-\d{2}-\d{2}', shema)[0]
        dict_locator['xlink_href'] = each_loc.get('xlink:href')
        dict_locator['shema'] = shema
        dict_locator['label_for_join'] = each_loc.get('xlink:href').split(sep='#')[1]
        dict_locator['loc_label'] = each_loc.get('xlink:label')
        list_locator.append(dict_locator)

    # link:locタグの取得結果をDFに    
    df_locator = pd.DataFrame(list_locator)

    # link:labelArcタグのみ抽出
    link_arc = soup.find_all('link:labelarc')

    # link:labelArcタグの取得結果をdictにし、dictを格納するカラのリストを作成
    list_arc = []

    # link:labelArcタグの情報をループ処理で取得
    for each_arc in link_arc:
        dict_arc = {}
        dict_arc['arc_role'] = each_arc.get('xlink:arcrole')
        dict_arc['loc_label'] = each_arc.get('xlink:from')
        dict_arc['xlink_label'] = each_arc.get('xlink:to')
        list_arc.append(dict_arc)

    # link:labelArcタグの取得結果をDFに    
    df_arc = pd.DataFrame(list_arc)

    # link:labelタグのみ抽出
    link_label = soup.find_all('link:label')

    # link:labelタグの取得結果をdictにし、dictを格納するカラのリストを作成
    list_label = []

    # link:labelタグの情報をループ処理で取得
    for each_label in link_label:
        dict_label = {}
        dict_label['xlink_label'] = each_label.get('xlink:label')
        dict_label['xlink_role'] = each_label.get('xlink:role')
        dict_label['xml_lang'] = each_label.get('xml:lang')
        dict_label['label_text'] = each_label.text
        list_label.append(dict_label)

    # link:labelタグの取得結果をDFに    
    df_label = pd.DataFrame(list_label)

    # locとarcの結合
    df_merged = pd.merge(df_locator, df_arc, on='loc_label', how='inner')
    # loc, arcとlabelの結合
    df_merged = pd.merge(df_merged, df_label, on='xlink_label', how='inner')

    return df_merged

def get_tse_grobal_label(arg_path):
    # labファイルの読み込み
    with open(arg_path, encoding='utf-8') as f:
        soup = BeautifulSoup(f.read(), 'lxml')

    # link:locタグのみ抽出
    link_loc = soup.find_all('loc')

    # link:locタグの取得結果をdictにし、dictを格納するカラのリストを作成
    list_locator = []

    # link:locタグの情報をループ処理で取得
    for each_loc in link_loc:
        dict_locator = {}
        shema = each_loc.get('xlink:href').split(sep='#')[0]
        dict_locator['xmlns_jpcrp_ymd'] = re.findall(r'\d{4}-\d{2}-\d{2}', shema)[0]
        dict_locator['xlink_href'] = each_loc.get('xlink:href')
        dict_locator['shema'] = shema
        dict_locator['label_for_join'] = each_loc.get('xlink:href').split(sep='#')[1]
        dict_locator['loc_label'] = each_loc.get('xlink:label')
        list_locator.append(dict_locator)

    # link:locタグの取得結果をDFに    
    df_locator = pd.DataFrame(list_locator)

    # link:labelArcタグのみ抽出
    link_arc = soup.find_all('labelarc')

    # link:labelArcタグの取得結果をdictにし、dictを格納するカラのリストを作成
    list_arc = []

    # link:labelArcタグの情報をループ処理で取得
    for each_arc in link_arc:
        dict_arc = {}
        dict_arc['arc_role'] = each_arc.get('xlink:arcrole')
        dict_arc['loc_label'] = each_arc.get('xlink:from')
        dict_arc['xlink_label'] = each_arc.get('xlink:to')
        list_arc.append(dict_arc)

    # link:labelArcタグの取得結果をDFに    
    df_arc = pd.DataFrame(list_arc)

    # link:labelタグのみ抽出
    link_label = soup.find_all('label')

    # link:labelタグの取得結果をdictにし、dictを格納するカラのリストを作成
    list_label = []

    # link:labelタグの情報をループ処理で取得
    for each_label in link_label:
        dict_label = {}
        dict_label['xlink_label'] = each_label.get('xlink:label')
        dict_label['xlink_role'] = each_label.get('xlink:role')
        dict_label['xml_lang'] = each_label.get('xml:lang')
        dict_label['label_text'] = each_label.text
        list_label.append(dict_label)

    # link:labelタグの取得結果をDFに    
    df_label = pd.DataFrame(list_label)
    df_locator.to_csv('df_locator.csv')
    df_arc.to_csv('df_arc.csv')
    # locとarcの結合
    df_merged = pd.merge(df_locator, df_arc, on='loc_label', how='inner')
    # loc, arcとlabelの結合
    df_merged = pd.merge(df_merged, df_label, on='xlink_label', how='inner')

    return df_merged
    

########## タクソノミファイルのファイルパスを指定 ##########
path_taxonomy = './taxonomy/tdnet/'
print(os.path.isdir(path_taxonomy))

# 各labファイルのパスを格納するリストの定義
list_path_taxonomy = []

# 各labファイルの検索
# list_path_taxonomy.extend(glob.glob(path_taxonomy + '**/jpcrp/**/label/**_lab.xml', recursive=True)) # 日本基準用タクソノミ
# list_path_taxonomy.extend(glob.glob(path_taxonomy + '**/jppfs/**/label/**_lab.xml', recursive=True)) # IFRS用タクソノミ
# list_path_taxonomy.extend(glob.glob(path_taxonomy + '**/jpigp/**/label/**_lab.xml', recursive=True)) # 米国基準用タクソノミ
list_path_taxonomy.extend(glob.glob(path_taxonomy + '**/**-lab.xml', recursive=True)) # 決算短信用タクソノミ

for filePath in list_path_taxonomy:
    dirfile = os.path.isfile(filePath)
    print(dirfile)
    df_global_label = get_tse_grobal_label(filePath)

    ########## taxonomy_global_label.tsvが保存されているパスを指定 ##########
    path_global_label = './taxonomy_tdnet_label.tsv'

    # 既存ファイル（taxonomy_global_label.tsv）の末尾に最新マスタを追記
    df_global_label.to_csv(path_global_label, sep ='\t', encoding='UTF-8', mode='a', header=False, index=False)
