import json
import time 
import urllib
import string
import datetime
import psycopg2
from uuid import uuid4
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.firefox.options import Options
from googleapiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
import gspread

def GoogleListUpdate(doc_Name,list_Name,dataframe, offsetright=1, offsetdown=1):
    """this fucn inserts Dataframe to the set google sheets"""
    edex = len(list(dataframe.axes[0]))
    eter = len(list(dataframe.axes[1]))
    print(edex,eter)
    sh = gc.open(doc_Name)
    worksheet1 = sh.worksheet(list_Name)
    for dexlet in range(0,eter):
        i = 0
        cell_list = worksheet1.range(offsetdown,dexlet+offsetright,edex+offsetdown-1,dexlet+offsetright)
        for cell in cell_list:
            insert_data = int(dataframe[dexlet][i]) if str(dataframe[dexlet][i]).isdigit() else dataframe[dexlet][i]
            cell.value = insert_data
            i+=1  
        worksheet1.update_cells(cell_list)

def GoogleListextract(doc_Name,list_Name):
    """this fucn exctract list of list of google sheets"""
    sh = gc.open(doc_Name)
    worksheet1 = sh.worksheet(list_Name)
    values_list = worksheet1.row_values(1)
    end_ls = []
    for i in range(1,len(values_list)+1):
        end_ls.append(list(worksheet1.col_values(i)))

    return end_ls

def modify_log(old_log, new_log):
    """this merges data form previos log and new logs"""
    heads_old = { head[0]:num for num,head in enumerate(old_log)}
    heads_new  = { head[0]:num for num,head in enumerate(new_log)}
    
    for i in old_log:
        if i[0] not in heads_new:
            i.insert(1,0)

    for i in new_log:
        if i[0] in heads_old:
            old_row = heads_old[i[0]]
            old_log[old_row].insert(1,i[1])
        else:
            old_log.append(i)
    return pd.DataFrame(old_log).transpose()

SCOPES = ['https://www.googleapis.com/auth/analytics.readonly',
             'https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/drive']

credentials = ServiceAccountCredentials.from_json_keyfile_name('kalmuktech-5b35a5c2c8ec.json', SCOPES)
gc = gspread.authorize(credentials)

conf = GoogleListextract('media_FM log','pass')
passW = json.loads(conf[0][0])
dates = conf[0][1]
last_date = datetime.datetime.strptime(dates, '%Y-%m-%d').date()
next_date = last_date + datetime.timedelta(days = 1)
conf[0][1] = str(next_date)
conf[0][0] = json.dumps(passW)
new_date_log = modify_log([],conf)
GoogleListUpdate('media_FM log','pass',new_date_log)

timer = datetime.datetime.now()

def get_data(quer,passW):
    # # Create a cursor object
    conn = psycopg2.connect(**passW)
    cur = conn.cursor()

    # A sample query of all data from the "vendors" table in the "suppliers" database
    cur.execute(quer)
    query_results = cur.fetchall()


    # Close the cursor and connection to so the server can allocate
    # bandwidth to other requests
    cur.close()
    conn.close()
    return query_results

def add_data(quer,passW):
    # # Create a cursor object
    conn = psycopg2.connect(**passW)
    cur = conn.cursor()

    # A sample query of all data from the "vendors" table in the "suppliers" database
    cur.execute(quer)
#     query_results = cur.fetchall()
    conn.commit()

    # Close the cursor and connection to so the server can allocate
    # bandwidth to other requests
    cur.close()
    conn.close()
    return True


genders = [
    'female',
    'male']
age = [
      '17',
      '18',
      '25',
      '35',
      '45',
      '55']

log_table = [["Дата выполнения",str(datetime.datetime.today())[:19]]]

options = Options()
options.headless = True
driver = webdriver.Firefox(options=options)
driver.get("http://google.com/")
print("Headless Firefox Initialized")
print(str(next_date))
driver.get("https://yandex.ru/")
ses = {'name': 'Session_id',
  'value': '3:1605474056.5.0.1600760878817:Mz6HWA:86.1|490782294.567919.2.2:567919|1130412051.1206811.2.2:1206811|225899.843718.zNmkECKd_JPnBviEvSXwPoft66g',
  'path': '/',
  'domain': '.yandex.ru',
  'secure': False,
  'httpOnly': False,
  'expiry': 2147483658,
  'sameSite': 'None'} 
driver.add_cookie(ses)

def get_list_of_params(items_text, gender, age):
    text_load = items_text.split("\n")
    head, lis_items = text_load[0],text_load[1:]
    res = []
    for i in range(0, len(lis_items),3):
        res.append([lis_items[i:i+3],gender,age])
    return res


lists = []
for i in genders:
    for j in age:
        datalog = [f'Группа {i} {j}']
        try:
            datalog = [f'Группа {i} {j}']
            filters = {'values': [{'id': 'startDates',
                       'data': {'from': str(next_date),
                        'to': str(next_date),
                        'name': 'month',
                        'inverted': False}},
                      {'id': 'userGender',
                       'data': {'inverted': False,
                        'values': [{'value': i, 'operator': '=='}]}},
                      {'id': 'userAge',
                       'data': {'inverted': False,
                        'values': [{'value': j, 'operator': '=='}]}}]}
            filters = urllib.parse.quote(str(filters))

            filters = filters.replace("False",'false')
            filters = filters.replace(" ","")
            filters = filters.replace("'",'"')
            filters = filters.replace(" ","")
            filters = filters.replace("%27","%22")

            base = "https://appmetrica.yandex.ru/statistic?appId=3532552&report=profiles-list&filters="
            params = "&metrics=ym_p_users&sampling=1&selectedAttributes=ym_p_profile"
            url = base+filters+params
            driver.get(url)

            done = False
            first = True
            while not done:

                time.sleep(3)
                try:
                    profiles = driver.find_element_by_css_selector('.profiles-list-table__table')
                    list_of_params=get_list_of_params(profiles.text,i,j)
                    print(len(list_of_params))
                except:
                    time.sleep(3)
                    try:
                        profiles = driver.find_element_by_css_selector('.profiles-list-table__table')
                        list_of_params=get_list_of_params(profiles.text,i,j)
                    except:
                        continue
                try:
                    driver.find_element_by_css_selector('.profiles-list-table__show-more-button').click()
                except:
                    print("buttons not here once")
                    if first:
                        first = False
                        time.sleep(2)
                        print("button not here twice")
                    else:
                        done = True
            lists.append(list_of_params)
            datalog.append(len(list_of_params)*3)
        except:
            datalog.append(str(sys.exc_info()[1]))
        log_table.append(datalog)
final_list = []
for i in lists:
    final_list.extend(i)
ker = {}
dirty_table = []
for i in final_list:
    for j in i[0]:
        if '-' in j:
            if j in ker:
                continue
            ker[j]=1
            end = [j,i[-1], i[-2]]
            dirty_table.append(end)
ker = {}
dirty_table = []
for i in final_list:
    for j in i[0]:
        if '-' in j:
            if j in ker:
                continue
            ker[j]=1
            end = [j,i[-1], i[-2]]
            dirty_table.append(end)
print('Грязная табилца уникальные значения')
print(len(dirty_table))
log_table.append(['Получено неуникальных заначений',len(dirty_table)])
dd = get_data('select distinct uuid from user_dimorphics',passW)
exists = [i[0] for i in dd]
clean_table = [i for i in dirty_table if i[0] not in exists ]
print('Чистая табилца уникальные значения')
print(len(clean_table))  
log_table.append(['Получено уникальных заначений',len(clean_table)])
queru =  """INSERT INTO user_dimorphics(id, uuid, sex, age_group, created_at)
VALUES"""
insert_time = datetime.datetime.today()
for i in clean_table:
    dems = []
    queru += f"('{uuid4()}', '{i[0]}', '{i[2]}','{i[1]}', '{insert_time}'),"
queru = queru[:-1]
queru += ";"
add_data(queru,passW)

timetook = str(datetime.datetime.now() - timer)
log_table.append(['Выполнение заняло',timetook])
log = GoogleListextract('media_FM log','log')
new_log = modify_log(log,log_table)
GoogleListUpdate('media_FM log','log',new_log)