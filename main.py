#!/usr/bin/python
# -*- coding: utf-8 -*-
import concurrent.futures
import os
import ssl
import time
import pymongo
import requests
import tqdm
import urllib3
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait, Select
from webdriver_manager.chrome import ChromeDriverManager

if not os.environ.get('PYTHONHTTPSVERIFY', '') and getattr(ssl,
                                                           '_create_unverified_context', None):
    ssl._create_default_https_context = ssl._create_unverified_context


def chunks(l, n):  # функция для разделения списка на саб списки n-размера
    for i in range(0, len(l), n):
        yield l[i:i + n]


def printProgressBar(iteration, total, prefix='', suffix='', decimals=1, length=100, fill='█'):  # прогресс бар
    percent = ("{0:." + str(decimals) + "f}").format(100 * (iteration / float(total)))
    filledLength = int(length * iteration // total)
    bar = fill * filledLength + '-' * (length - filledLength)
    print('\r%s |%s| %s%% %s' % (prefix, bar, percent, suffix), end='\r')
    if iteration == total:
        print()


def extract(r_html, collection):  # для сбора URL-ов страниц с протоколами
    mycollection = collection.find().distinct("URL")  # подключается к базе к списку с обработанными URL
    url_main = 'https://goszakup.gov.kz'
    soup = BeautifulSoup(r_html, 'html.parser')
    data_text = soup.select('td > a')
    global links_num
    for elem in data_text:
        links_num = links_num + 1
        printProgressBar(links_num, int(2892), "Progress:", "Complete")
        newurl = url_main + ''.join(elem.get_attribute_list('href')) \
                 + '?tab=protocols' + '\n'
        if newurl not in mycollection:  # сравнивает новые URL с обработанными URL, если они уже в базе, игнорирует
            url_links.append(newurl)


def download_data(line):  # функция скачивает URL'ы к самим протоколам
    url_2 = line.strip()
    q = requests.get(url_2, verify=False)
    q.encoding = 'utf-8'
    q_html = q.text
    soup = BeautifulSoup(q_html, 'html.parser')
    mylink = soup.find('b', text='Протокол итогов')
    mylink = mylink.parent.parent
    mylink = mylink.find('a', class_='btn btn-sm btn-primary')
    return ''.join(mylink.get_attribute_list('href')) + '\n'


def addToDatabase(url):  # функция создает JSON объект с данными
    mydict = []
    url = url.strip()
    r = requests.get(url, verify=False)
    r.encoding = 'utf-8'
    r_html = r.text
    if r_html is not None:
        soup = BeautifulSoup(r_html, 'html.parser')
        myid = soup.findAll('h2')
        myid = str(myid[1].text)
        myid = myid.replace('Протокол об итогах №', '')
        myid = myid.strip()
        mylink = soup.find('caption', text='Расчет условных цен участников конкурса:')
        if mylink is not None:
            mylink = mylink.parent.findAll('td')
            myitems = []
            for element1 in mylink:
                myitems.append((element1.text).strip())
            myitems = myitems[0:11:1]
            del myitems[0]
            mydict = dict([("_id", myid), ("Company name", myitems[0]),  # создает структуры данных
                           ("Company BIN", myitems[1]), ("SUM$", myitems[2]),
                           ("COMP$", myitems[3]), ("FIN$", myitems[6])])
            # print(mydict.copy())
    return mydict


url_links = []
links_num = 0

myclient = pymongo.MongoClient("mongodb://localhost:27017/")  # подключение к базе mongoDB
mydb = myclient["mydatabase"]
mycol = mydb["test2"]  # подключение к нужной collections в базе данных
banurl = mydb["banned_urls"]  # подключение к базе обработанных URL

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)  # disables warnings in python
driver = webdriver.Chrome(ChromeDriverManager().install())  # uses webdriver-manager to open chrome
url = \
    'https://goszakup.gov.kz/ru/search/announce?filter%5Bname%5D=%D0%A0%D0%B0%D0%' \
    'B7%D1%80%D0%B0%D0%B1%D0%BE%D1%82%D0%BA%D0%B0+%D0%9F%D0%A1%D0%94&filter%5Bcus' \
    'tomer%5D=&filter%5Bnumber%5D=&filter%5Byear%5D=&filter%5Bstatus%5D%5B%5D=350&fi' \
    'lter%5Bmethod%5D%5B%5D=2&filter%5Bamount_from%5D=&filter%5Bamount_to%5D=&filter%5B' \
    'trade_type%5D=&filter%5Bstart_date_from%5D=&filter%5Bstart_date_to%5D=&filter%5Ben' \
    'd_date_from%5D=&filter%5Bend_date_to%5D=&filter%5Bitog_date_from%5D=&filter%5Bitog_' \
    'date_to%5D=&smb='
driver.get(url)
select = \
    Select(driver.find_element_by_xpath("//select[@class='form-control m-b-sm']"))
select.select_by_visible_text('2000')  # выбирает по сколько ссылок показывать на странице
WebDriverWait(driver, 1)  # время для подгрузки страницы в 1 сек
driver.get(driver.current_url)
r_html = driver.page_source
extract(r_html, banurl)
pages = \
    driver.find_elements_by_xpath(
        "//ul[@class='pagination m-t-xxs']/li/a")  # после поиска в масштабе 2000 на страницу, есть только две
# страницы, webdriver преходит к последней странице
pages[-1].click()  # имитация нажатия для взаимодействия с js
driver.get(driver.current_url)
r_html = driver.page_source
extract(r_html, banurl)  # обработка URL'ов с последней страницы
driver.quit()
with open('all_urls.txt', 'w') as full:  # для теста создаю документ в котором храню URL-ы
    for lines in url_links:
        full.write(str(lines).strip() + '\n')

start = time.time()  # для трекинга времени обработки кода
listofdict = []  # создаю list, для объединения всех будущих обработанных dict

with concurrent.futures.ProcessPoolExecutor() as executor:  # через concurrent.futures эта часть кода будет обрабатываться всеми ядрами ЦП
    for (ssss, bbbb) in tqdm.tqdm(zip(url_links, executor.map(download_data, url_links)), total=len(url_links)):
        # tqdm() создает прогресс бар
        try:
            b1 = addToDatabase(bbbb)
            if b1:  # если в протоколах не объявлен победитель, то создаются None объекты, чтобы ими не засорять базу, проверка if
                b1["Urls"] = ssss
                listofdict.append(b1.copy())  # так как b1 это dict, надо создать копию чтобы b1 не менялся
        except:
            print("error")  # некоторые протоколы в pdf формате, потому bs4 не может их обработать,
            # выдает TypeError, чтобы код работал сделал exception-handler

with open("mydatabase.txt", 'w') as dfile:  # для дебагинга создаю txt файл с базой данных
    for elem in listofdict:
        if elem:
            dfile.write(str(elem) + "\n")

try:
    p = []  #
    collection = banurl.find()  #
    for elem in url_links:  #
        s = {"URL": elem}  # вношу новые обработанные URL'ы в базу
        p.append(s.copy())  #
    lst3 = [value for value in p if value not in collection]  #
    banurl.insert_many(kkk for kkk in lst3)  #

    s = listofdict.copy()  #
    collection = mycol.find()  #
    lst3 = [value for value in s if value not in collection]  # вношу сами данные в базу
    mycol.insert_many(kkk for kkk in lst3)  #
    print('success')  #
except:
    print('Failed to write to database')

end = time.time()
print('Execution time: ' + str(
    end - start))  # базу с 2892 объектами обрабатывает примерно за 2000сек, в зависимости от скорости интернета
# может справится за 900 сек

