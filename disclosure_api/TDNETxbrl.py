import urllib.request, urllib.error
from bs4 import BeautifulSoup
from datetime import date
import time
import requests
import os
from UsuallyFunction import UsuallyFunction as uf
class XBRLrequest:

    def __init__(self, dte:date) -> None:
        self.dte = dte
        self.saveDir = f'./ZIP/{dte.strftime("%Y%m%d")}'
    
    def __link_check(self, url: str):
        try:
            f = urllib.request.urlopen(url)
            return True
        except:
            return False
        finally:
            if "f" in locals():
                f.close()
    
    def getXBRL_link(self):
        flag = True
        n = 0
        while flag == True:
            n += 1
            url =f'https://www.release.tdnet.info/inbs/I_list_{n:03}_{self.dte.strftime("%Y%m%d")}.html'
            flag = self.__link_check(url)
            if flag == True:
                url_txt = requests.get(url).text
                soup = BeautifulSoup(url_txt, 'html.parser')
                for el in soup.find_all('div', class_='xbrl-mask'):
                    if not os.path.exists(self.saveDir):
                        os.makedirs(self.saveDir)
                    file_name = el.a["href"]
                    xbrl_link = f'https://www.release.tdnet.info/inbs/{file_name}'
                    print(xbrl_link)
                    urllib.request.urlretrieve(xbrl_link, f'{self.saveDir}/' + file_name)
                time.sleep(1)
         
for dte in uf.date_range(date(2022,11,3),date.today()):
    dte = date.today()
    print(dte.strftime("%Y年%m月%d日"))
    model = XBRLrequest(dte)
    model.getXBRL_link()
