from xbrl_read import XbrlRead
import pandas as pd
import uuid
import time

if __name__ == "__main__":
    
    zippath = "D:\\ZIP\\20221027\\081220221013544068.zip"
    
    xr = XbrlRead(zippath)    
    
    # 財務諸表
    label_df = xr.add_label_df() 
    
    # 計算リンク 
    start = time.time()
    cal_df = xr.to_cal_link_df()
    
    # 表示リンク
    start = time.time()
    pre_df = xr.to_pre_link_df() 
    
    # 定義リンク   
    start = time.time()
    def_df = xr.to_def_link_df()
    
    # ドキュメントフォルダを空にする
    import shutil
    import os
    tax = os.path.join(os.path.dirname(os.path.abspath(__file__)),r'doc\taxonomy')
    print(os.path.isdir(tax))