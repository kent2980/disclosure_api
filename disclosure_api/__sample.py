import pymysql.cursors
from pathlib import Path
import os
from disclosure_api.xbrl_read import XbrlRead

# ************************************************************
# データベース接続**********************************************
# ************************************************************
connection = pymysql.connect(host='localhost',
                             user='****',
                             password='******',
                             database='*****',
                             cursorclass=pymysql.cursors.DictCursor)

with connection:
    with connection.cursor() as cursor:
        # レコードを挿入
        sql = 'INSERT IGNORE INTO xbrl_order \
            VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
        cal_sql = 'INSERT IGNORE INTO xbrl_cal_link \
            VALUES(%s,%s,%s,%s,%s,%s,%s,%s)'
        pre_sql = 'INSERT IGNORE INTO xbrl_pre_link \
            VALUES(%s,%s,%s,%s,%s,%s,%s)'
        def_sql = 'INSERT IGNORE INTO xbrl_def_link \
            VALUES(%s,%s,%s,%s,%s,%s,%s)'

        # ************************************************************
        # データ取得***************************************************
        # ************************************************************

        zip_path = Path("D:/ZIP/")
        zip_list = list(zip_path.glob("**/*.zip"))
        for zip_file in zip_list:

            print(zip_file)

            play = XbrlRead(zip_file)
            df = play.add_label_df()
            file_name = os.path.splitext(os.path.basename(zip_file))[0]
            df.to_csv(f'D:/CSV/label/{file_name}.csv')
            cursor.executemany(sql,df.values.tolist())
            cursor.executemany(cal_sql, play.to_cal_link_df().values.tolist())
            cursor.executemany(pre_sql, play.to_pre_link_df().values.tolist())
            cursor.executemany(def_sql, play.to_def_link_df().values.tolist())
            connection.commit()