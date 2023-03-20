from datetime import datetime
import re
import jaconv

def format_date(date_str):
    """
    日付文字列を受け取り、統一的なフォーマット 'YYYY-MM-DD' に変換する。

    Args:
        date_str (str): "令和4年10月3日"、"2023年10月3日"、"2023-10-3" のいずれかの形式の日付文字列。

    Returns:
        str: 'YYYY-MM-DD' 形式の日付文字列。

    Raises:
        ValueError: 引数がサポートされていない形式の日付文字列の場合。

    Examples:
        >>> format_date('令和4年10月3日')
        '2022-10-03'
        >>> format_date('2023年10月3日')
        '2023-10-03'
        >>> format_date('2023-10-3')
        '2023-10-03'
    """
    try:
        pattern = r"[年月日]"
        value = re.split(pattern, date_str)

        jc_to_ad = {'昭和': 1925, '平成': 1988, '令和': 2019}
        pattern = r"\d+"
        value[0] = str(int(jc_to_ad[re.sub(pattern, '', value[0])]) + int(re.findall(pattern,value[0])[0]))
        date_obj = datetime.strptime(value[0]+value[1]+value[2],'%Y%m%d').strftime('%Y-%m-%d')
        
    except KeyError:
        try:
            # "YYYY年MM月DD日"のフォーマット
            date_obj = datetime.strptime(date_str, '%Y年%m月%d日').strftime('%Y-%m-%d')
        except ValueError:
            # "YYYY-MM-DD"のフォーマット
            date_obj = datetime.strptime(date_str, '%Y-%m-%d').strftime('%Y-%m-%d')
        
        
    return date_obj

date_str1= "令和4年10月3日"
standard_date1= format_date(date_str1)
print(standard_date1)  # Output: "2022-10-3"

date_str2 = "2022年10月3日"
standard_date2 = format_date(date_str2)
print(standard_date2)  # Output: "2022-10-3"

date_str3 = "2022-10-3"
standard_date3 = format_date(date_str3)
print(standard_date3)  # Output: "2022-10-3"

date_str4 = "2022 年 10 月 26 日"
date_str4 = jaconv.z2h(date_str4, kana=False, digit=True, ascii=True)
standard_date2 = format_date(date_str2)
print(standard_date2)  # Output: "2022-10-3"