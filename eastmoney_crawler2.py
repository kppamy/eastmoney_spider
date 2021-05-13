"""
e.g: http://data.eastmoney.com/bbsj/201912/lrb.html
"""
import requests
import re
from multiprocessing import Pool
import json
import csv
import pandas as pd
import os
import time
from datetime import datetime
import qutils.futils as fu

# FontMapping= [{code: "&#xEEC5;", value: 1},
#               {code: "&#xECEA;", value: 2},
#               {code: "&#xEA5D;", value: 3},
#               {code: "&#xF78F;", value: 4},
#               {code: "&#xEBED;", value: 5},
#               {code: "&#xF2FF;", value: 6},
#               {code: "&#xF4CD;", value: 7},
#               {code: "&#xF2F8;", value: 8},
#               {code: "&#xE4E5;", value: 9},
#               {code: "&#xE268;", value: 0}],





# "&#xE793;" : 1,
# "&#xECEA;":2,
# "&#xF3C3;":3,
# "&#xE891;" :5
# "&#xE7A3;": 6,
# "&#xF05A;" :7,
# "&#xE8BC;" :8
#
#
# "&#xE268;":4,
# "&#xECEA";5,
#
# "&#xECD9;" :1,
# "&#xF3C3;" :2,






PLEDGE_DICT={
    # 0-50
    "&#xF05A;":1,
    "&#xECEA;":2,
    "&#xE793;":3,
    "&#xF137;":4,
    "&#xEE3A;":5,
    "&#xECD9;":6,
    "&#xE375;":7,
    "&#xECE9;":8,
    "&#xE0D4;":9,
    "&#xF78F;":0,
    # 50-100
    "&#xF275;": 1,
    "&#xEBC0;": 2,
    "&#xECD9;": 3,
    "&#xE0D4;": 4,
    "&#xF2FF;": 5,
    "&#xF3C3;": 6,
    "&#xE4E5;": 7,
    "&#xF4CD;": 8,
    "&#xE268;": 9,
    "&#xE712;": 0
}


PLEDGE_DICT3 = {
    "&#xE80C;": 1,
    "&#xE268;": 2,
    "&#xEE3A;": 3,
    "&#xE793;": 4,
    "&#xF3C3;": 5,
    "&#xE375;": 6,
    "&#xE891;": 7,
    "&#xE712;": 9,
    "&#xF137;": 0
}





PLEDGE_DICT2={
    "&#xF275;": 1,
    "&#xEBC0;": 2,
    "&#xECD9;": 3,
    "&#xE0D4;": 4,
    "&#xF2FF;": 5,
    "&#xF3C3;": 6,
    "&#xE4E5;": 7,
    "&#xF4CD;": 8,
    "&#xE268;": 9,
    "&#xE712;": 0}


# 设置文件保存在D盘eastmoney文件夹下
file_path = './data/'
if not os.path.exists(file_path):
    os.mkdir(file_path)
os.chdir(file_path)


def get_int_input(instruction, default):
    # int表示取整，里面加float是因为输入的是str，直接int会报错，float则不会
    # https://stackoverflow.com/questions/1841565/valueerror-invalid-literal-for-int-with-base-10
    var = input(instruction+'\n')
    if var.isdigit():
        res = int(var)
    elif var == '':
        res = default
    return res


def convert(text):
    for i in PLEDGE_DICT: text=text.replace(i,str(PLEDGE_DICT[i]))
    return text

# 1 设置表格爬取时期
def set_table():
    print('*' * 80)
    print('\t\t\t\teast money')
    print('Author  Cay\'s Day Dream 2020.08.27')
    print('--------------')

    # 1 设置财务报表获取时期

    lastyear = datetime.today().year - 1

    year = get_int_input('Please input year： ', lastyear)

    quarter = get_int_input('Which quarter: (1: First quarter: ，2: Second quarter，3：Third Quarter，4: Year report)：', 4)

    #
    # quarter = int(float(input('请输入小写数字季度(1:1季报，2-年中报，3：3季报，4-年报)：\n')))
    # if quarter == '':
    #     quarter = 4
    # while (quarter < 1 or quarter > 4):
    #     quarter = int(float(input('季度数值输入错误，请重新输入：\n')))

    # 转换为所需的quarter 两种方法,2表示两位数，0表示不满2位用0补充，
    # http://www.runoob.com/python/att-string-format.html
    quarter = '{:02d}'.format(quarter * 3)
    # quarter = '%02d' %(int(month)*3)

    # 确定季度所对应的最后一天是30还是31号
    if (quarter == '06') or (quarter == '09'):
        day = 30
    else:
        day = 31
    date = '{}-{}-{}' .format(year, quarter, day)
    # print('date:', date)  # 测试日期 ok

    # 2 设置财务报表种类
    # tables = int(
    #     input('请输入查询的报表种类对应的数字(1-业绩报表；2-业绩快报表：3-业绩预告表；4-预约披露时间表；5 financial_statement；6-利润表；7-现金流量表): \n'))

    tables = int(
        input('Which type of statement do you want: \n (1 performance_statement； 2 early_performance_statement;'
              '3 performance_forecast_statement；4 date_announce_statement；\n''5 financial_statement；'
              '6 profit_statement；7 cash_flow_statement; 8 stock_pledge； 9 dividend;\n'))

    dict_tables = {1: 'performance_statement', 2: 'early_performance_statement', 3: 'performance_forecast_statement',
                   4: 'date_announce_statement', 5: 'financial_statement', 6: 'profit_statement', 7: 'cash_flow_statement'
                   , 8: 'stock_pledge', 9: 'dividend_data'}

    dict = {1: 'YJBB', 2: 'YJKB', 3: 'YJYG',
            4: 'YYPL', 5: 'ZCFZB', 6: 'LRB', 7: 'XJLLB', 8: 'ZD_QL_LB',9:'DCSOBS'}
    category = dict[tables]
    category_type = ''
    st = ''
    # js请求参数里的type，第1-4个表的前缀是'YJBB20_'，后3个表是'CWBB_'
    # 设置set_table()中的type、st、sr、filter参数
    if tables == 1:
        category_type = 'YJBB20_'
        st = 'latestnoticedate'
        sr = -1
        filter =  "(securitytypecode in ('058001001','058001002'))(reportdate=^%s^)" %(date)
    elif tables == 2:
        category_type = 'YJBB20_'
        st = 'ldate'
        sr = -1
        filter = "(securitytypecode in ('058001001','058001002'))(rdate=^%s^)" %(date)
    elif tables == 3:
        category_type = 'YJBB20_'
        st = 'ndate'
        sr = -1
        filter=" (IsLatest='T')(enddate=^2018-06-30^)"
    elif tables == 4:
        category_type = 'YJBB20_'
        st = 'frdate'
        sr = 1
        filter =  "(securitytypecode ='058001001')(reportdate=^%s^)" %(date)
    elif tables == 8:
        st = 'amtshareratio'
        sr = -1
        filter = '(tdate=^2020-04-10^)'
    elif tables == 9:
        filter = "(reportdate=^%s^)" %(date)
        sr = -1
        st = 'YAGGR'
    else:
        category_type = 'CWBB_'
        st = 'noticedate'
        sr = -1
        filter = '(ReportingPeriod=^%s^)' % (date)

    category_type = category_type + category
    # print(category_type)
    # 设置set_table()中的filter参数

    yield{
    'date':date,
    'category':dict_tables[tables],
    'category_type':category_type,
    'st':st,
    'sr':sr,
    'filter':filter
    }


# 2 设置表格爬取起始页数
def page_choose(page_all):

    # 选择爬取页数范围
    start_page = get_int_input('Please enter page start No：', 1)
    nums = get_int_input('How many pages do you want? ', int(page_all.group(1)))
    end_page = start_page + nums

    # 返回所需的起始页数，供后续程序调用
    yield{
        'start_page': start_page,
        'end_page': end_page
    }


# 3 表格正式爬取
def get_table(date, category_type,st,sr,filter,page,url=None, retry=True):
    print('\nDownloading page No.%s' % page)
    # 参数设置
    params = {
        # 'type': 'CWBB_LRB',
        'type': category_type,  # 表格类型
        'token': '70f12f2f4f091e459a279469fe49eca5',
        'st': st,
        'sr': sr,
        'p': page,
        'ps': 50,  # 每页显示多少条信息
        'js': 'var LFtlXDqn={pages:(tp),data: (x)}',
        'filter': filter,
        # 'rt': 51294261  可不用
    }
    url = 'http://dcfm.eastmoney.com/em_mutisvcexpandinterface/api/js/get?'
    try:
        response = requests.get(url, params=params).text
    except Exception:
        if retry:
            print("\n Download error, retry after 1 s")
            time.sleep(1)
            response = requests.get(url, params=params).text
    # print(response)
    # from html.parser import HTMLParser
    # html_parser = HTMLParser()
    # response = html_parser.unescape(response)
    # # response.replace('&#x', '\\x').encode('utf-8').decode('unicode_escape')
    # print(response)

    if category_type == 'ZD_QL_LB' and page == 1:
        # for i in PLEDGE_DICT: response = response.replace(i, str(PLEDGE_DICT[i]))
        print(response)
        fu.save('response.txt',response)

    # 确定页数
    pat = re.compile('var.*?{pages:(\d+),data:.*?')
    page_all = re.search(pat, response)
    print(page_all.group(1))  # ok

    # 提取{},json.loads出错
    # pattern = re.compile('var.*?data: \[(.*)]}', re.S)

    # 提取出list，可以使用json.dumps和json.loads
    pattern = re.compile('var.*?data: (.*)}', re.S)
    items = re.search(pattern, response)
    # 等价于
    # items = re.findall(pattern,response)
    # print(items[0])
    data = items.group(1)
    data = json.loads(data)
    # data = json.dumps(data,ensure_ascii=False)

    return page_all, data,page


# 写入表头
# 方法1 借助csv包，最常用
def write_header(data,category):
    with open('{}.csv' .format(category), 'a', encoding='utf_8_sig', newline='') as f:
        headers = list(data[0].keys())
        # print(headers)  # 测试 ok
        writer = csv.writer(f)
        writer.writerow(headers)


def write_table(data,page,category):
    # 写入文件方法1
    for d in data:
        with open('{}.csv' .format(category), 'a', encoding='utf_8_sig', newline='') as f:
            w = csv.writer(f)
            w.writerow(d.values())


def main(date, category_type,st,sr,filter,page):
    func = get_table(date, category_type,st,sr,filter,page)
    data = func[1]
    page = func[2]
    write_table(data,page,category)


if __name__ == '__main__':
    # 获取总页数，确定起始爬取页数
    for i in set_table():
        date = i.get('date')
        category = i.get('category')
        category_type = i.get('category_type')
        st = i.get('st')
        sr = i.get('sr')
        filter = i.get('filter')

    constant = get_table(date,category_type,st,sr,filter, 1)
    page_all = constant[0]

    for i in page_choose(page_all):
        start_page = i.get('start_page')
        end_page = i.get('end_page')

    # 写入表头
    write_header(constant[1],category)
    start_time = time.time()  # 下载开始时间
    # 爬取表格主程序
    for page in range(start_page, end_page+1):
        main(date,category_type,st,sr,filter, page)
    
    end_time = time.time() - start_time  # 结束时间
    print('Download completed')
    print('Time cost: {:.1f} s' .format(end_time))
