import os
import sys
import glob

parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

folder_name = "disclosure_api"
target_dir = os.path.join(parent_dir, folder_name)

sys.path.append(target_dir)

from disclosure_api.xbrl_read import XbrlRead

# 日付フォルダのリスト
dirs = glob.glob(f'{"/home/kent2980/doc/tdnet"}/*/')
dirs = sorted(dirs)

for dir in dirs:

    # 開始メッセージ
    print(
        "\n************************************************************************")
    print(f" [{dir}]よりデータをインポートします。")
    print(
        "************************************************************************")

    # zip files
    files = glob.glob(f'{dir}/*.zip')
    files = sorted(files)

    # プログレスバーを設置
    with tqdm(total=len(files)) as bar:

        # *************************
        # データベース接続
        # *************************
        
        # コミットカウント
        i = 0

        for f in files:

            # XBRLの読み取り開始
            xr = XbrlRead(f)

            # ***************************
            # DataFrame取得
            # ***************************

            # 会社詳細
            explain_df = xr.company_explain_df()
            
            print(explain_df)