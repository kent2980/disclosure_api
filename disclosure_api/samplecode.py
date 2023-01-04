from xbrl_read import XbrlRead
import pandas as pd
import uuid
import time

if __name__ == "__main__":
    
    zippath = "D:\\ZIP\\20221027\\081220221013544068.zip"
    
    xr = XbrlRead(zippath)    
    
    # 財務諸表
    start = time.time()
    label_df = xr.add_label_df() 
    print(f'財務諸表:{time.time()-start}')
    
    # 計算リンク 
    start = time.time()
    cal_df = xr.to_cal_link_df()
    # 中間テーブル
    inter_cal_df = label_df.rename(columns={'id':'order_id'}).merge(cal_df.rename(columns={'id':'cal_id'}), how='right', on=['explain_id', 'doc_element', 'namespace', 'element'])
    inter_cal_df['id'] = pd.Series(str(uuid.uuid4()) for _ in range(len(inter_cal_df)))
    inter_cal_df = inter_cal_df[['id', 'order_id', 'cal_id']]
    print(f'計算リンク:{time.time()-start}')
    
    # 表示リンク
    start = time.time()
    pre_df = xr.to_pre_link_df() 
    # 中間テーブル
    inter_pre_df = label_df.rename(columns={'id':'order_id'}).merge(pre_df.rename(columns={'id':'pre_id'}), how='right', on=['explain_id', 'doc_element', 'namespace', 'element'])
    inter_pre_df['id'] = pd.Series(str(uuid.uuid4()) for _ in range(len(inter_pre_df)))
    inter_pre_df = inter_pre_df[['id', 'order_id', 'pre_id']]
    print(f'表示リンク:{time.time()-start}')
    
    # 定義リンク   
    start = time.time()
    def_df = xr.to_def_link_df()
    # 中間テーブル
    inter_def_df = label_df.rename(columns={'id':'order_id'}).merge(def_df.rename(columns={'id':'def_id'}), how='right', on=['explain_id', 'doc_element', 'namespace', 'element'])
    inter_def_df['id'] = pd.Series(str(uuid.uuid4()) for _ in range(len(inter_def_df)))
    inter_def_df = inter_def_df[['id', 'order_id', 'def_id']]
    print(f'定義リンク:{time.time()-start}')
