from Communication import *
from baseWebSocket import *
import globaldata as RealTimeSnapshot
from concurrent.futures import ThreadPoolExecutor as Pool
import concurrent.futures
import configparser
import os
import logging
import easyquotation
import tushare as ts
import datetime
import json
import pandas
import datetime
import time

# 读取配置
config = configparser.ConfigParser()
config.read('config.ini')

# ----------配置文件--------------------
# 行情本地服务地址（此服务通过easyquotation或tushare获取行情发送给客户端
host = config.get("configInformation", "host")
# 行情服务ip地址
ip = config.get("configInformation", "ip")
# 行情服务端口
port = config.getint("configInformation", "port")
#-----------
# 要获取的股票（深圳证券交易所、上海证券交易所）行情所在交易所信息 sz-深圳交易所 sh-上海交易所
change = config.get("configInformation", "change")
# 是否多线程处理，根据数据分组数量，划分不同线程处理
ismultthread = config.getboolean("configInformation", "ismultthread")
# 是否发送数据列表
issendList = config.getboolean("configInformation", "issendList")
# ---是否记录行情数据，如果是，记录到行情数据保存地址---
isrecorddata = config.getboolean("configInformation", "isrecorddata")
# ---行情数据保存地址---
dataPath = config.get("configInformation", "dataPath")

# 使用tushare或easyquotation行情源
datasource= config.get("configInformation","datasource")
# 单个查询代码最大量  tushare必须小于895，easyquotation无限制
querymaxcodes = config.getint("configInformation", "querymaxcodes")

# easyquotation行情查询单位 qq，sina，hkquote，具体查easyquotation use
easyquotationuse = config.get("configInformation","easyquotationuse")
# 上交所指标总数 easyquotation
shchangeindex = config.getint("configInformation", "shchangeindex")
# 是否记录错误日志
islog = config.getboolean("configInformation",  "islog")
# 显示当前配置信息
print("-----------行情广播服务-服务端信息-----------")
print("websocket地址：%s \n交易所:%s \n行情源：%s \n"
      "是否记录数据：%s \n是否多线程：%s  \n是否发送列表数据：%s" % (host, change, datasource, isrecorddata, ismultthread, issendList))
print("-------------------------------------------")
print("-------------------------------------------")
# ----------------------------------------------
# 初始化easyquotation行情单位
quotation = easyquotation.use(easyquotationuse)
# 初始化websocket发送服务
# factory = object()
# 代码列表
StockNoList = []
# 行情快照
RealTimeSnapshot._init()
RealTimeSnapshot.DataSource = datasource
RealTimeSnapshot.querymaxcodes = querymaxcodes


if islog:
    # 日志工具初始化
    FILE = os.getcwd()
    logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s:%(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt='%a, %d %b %Y %H:%M:%S',
                    filename=os.path.join(FILE, 'log.txt'),
                    filemode='w')


def getChangeCode():
    '''
    获取交易所交易代码
    '''
    global StockNoList
    try:

        # 获取交易所所有股票代码
        all_code = quotation.market_snapshot(prefix=True)
        StockNoList = []
        # 获取指定交易所所有股票代码
        # print(type(all_code))
        for (k, v) in all_code.items():
            # 遍历所有行情
            if change in k:
                # print(k)
                StockNoList.append(k.lstrip(change))
        # 添加上海指数代码
        if change == 'sh':
            for num in range(1, shchangeindex):
                StockNoList.append("sh" + str(num).zfill(6))

        strcode = ",".join(StockNoList)
        config.set("configInformation", "codes", strcode)
        config.write(open("config.ini", "w"))
    except Exception as ex:
        logging.error(ex, locals())


def codesGroup(codelist):
    '''
    按配置的单次最大查询数分组
    :param codelist:
    :return:
    '''
    # 计算行情分组
    sum_stock = len(codelist)
    sum_mod = sum_stock % querymaxcodes
    I = sum_stock // querymaxcodes
    if sum_mod > 0:
        I = I + 1

    # 记录分组行情代码
    threadcodes = []
    for i in range(0, I, 1):
        if (i + 1) * querymaxcodes > sum_stock:
            all_list = codelist[i * querymaxcodes: sum_stock]
            threadcodes.append(all_list)
            # print(i * querymaxcodes, sum_stock - 1)
        else:
            all_list = codelist[i * querymaxcodes: (i + 1) * querymaxcodes]
            # print(i * querymaxcodes, (i + 1) * querymaxcodes - 1)
            threadcodes.append(all_list)
    # news = input()
    return threadcodes


def iniSnapshotBytushare():
    '''
    # 初始化行情快照 通过tushare初始化行情快照
    :return:
    '''
    global RealTimeSnapshot, StockNoList
    try:
        codeslist = codesGroup(StockNoList)
        for codes in codeslist:
            # 获取行情数据
            all_list = ts.get_realtime_quotes(codes)
            for ix in all_list.index:
                # 遍历
                stock_code = all_list.loc[ix]['code']
                if not RealTimeSnapshot.Snapshot.__contains__(stock_code):
                    # 快照中不含有时，将行情写入快照
                    RealTimeSnapshot.Snapshot[stock_code] = all_list.loc[ix]
                    # 通过websocket广播数据
                    # mod = all_list.loc[ix].to_json()
                    # factory.broadcast(mod)
    except Exception as ex:
        logging.error(ex)


def iniSnapshotByeasyquotation():
    '''
    # 初始化行情快照 通过easyquotation初始化行情快照
    :return:
    '''
    global RealTimeSnapshot, StockNoList
    try:

        codeslist = codesGroup(StockNoList)
        for codes in codeslist:
            # 获取行情数据
            all_list = quotation.stocks(codes)
            for (k, v) in all_list.items():
                # 遍历
                stock_code = k
                if not RealTimeSnapshot.Snapshot.__contains__(stock_code):
                    # 快照中不含有时，将行情写入快照
                    RealTimeSnapshot.Snapshot[stock_code] = v
                    # 通过websocket广播数据
                    # v['code'] = k
                    # mod = json.dumps(v)
                    # factory.broadcast(mod)
    except Exception as ex:
        logging.error(ex)
        # print(v)


def websocketstart():
    '''
    启动websocket服务
    '''
    global RealTimeSnapshot
    cTool = CommunicationTool()
    # 传入webSocket的地址
    cTool.setFlag(CommunicationTool.Server)
    # 设置类型

    cTool.setServerProtocol(BaseSipServerProtocol)
    RealTimeSnapshot.factory = BaseSipServerFactory(host)
    cTool.setServerFactory(RealTimeSnapshot.factory)
    # factory.setProtocolOptions(autoPingInterval=5)
    # 设置是否自动断线重连
    cTool.setAutoReconnect(False)
    # 创建server
    cTool.createServer(ip, port)
    # 开启server监听
    cTool.startListen()


    print("WebSocket服务已启动.")


def Checktime(starttime, endtime, weibotime):
    Flag = False
    starttime = time.strptime(starttime, '%Y-%m-%d %H:%M:%S')
    #'%Y-%m-%d %H:%M:%S'
    endtime = time.strptime(endtime, '%Y-%m-%d %H:%M:%S')
    weibotime = time.strptime(str(weibotime),'%Y-%m-%d %H:%M:%S')
    if int(time.mktime(starttime)) <= int(time.mktime(weibotime)) and int(time.mktime(endtime)) >= int(time.mktime(weibotime)):
        Flag=True
    else:
        Flag=False
    return Flag


def isTradingTime():
    '''
    判断是否是交易时间
    :return: True 是交易时间 False 不是交易时间
    '''
    curNow = datetime.datetime.now()

    yyy = curNow.strftime('%Y-%m-%d %H:%M:%S')
    curY = str(curNow.year)
    curM = str(curNow.month)
    curD = str(curNow.now().day)
    flagMo = Checktime(curY + '-' + curM +'-'+ curD +' 9:00:00', curY + '-' + curM +'-'+ curD +' 11:50:00',
                       curNow.strftime('%Y-%m-%d %H:%M:%S'))
    flagAf = Checktime(curY + '-' + curM + '-' + curD + ' 12:50:00', curY + '-' + curM + '-' + curD + ' 15:40:00',
                       curNow.strftime('%Y-%m-%d %H:%M:%S'))

    return flagAf | flagMo


def websocketstop():
    pass


def sendListDatabylistTushare(codelist):
    '''
    根据代码列表获取行情并发送数据 行情源：tushare
    :param codelist: 代码列表
    :return:
    '''
    global RealTimeSnapshot

    all_list_code = codelist
    all_list = ts.get_realtime_quotes(all_list_code)
    resultdict = {}
    for ix in all_list.index:
        try:
            stock_code = all_list.loc[ix]['code']
            mod = all_list.loc[ix].to_json()
            # print(len(RealTimeSnapshot.factory._connectionSets))
            # RealTimeSnapshot.factory.broadcast(mod)
            # resultdict[stock_code] = all_list.loc[ix].to_json()
            if not RealTimeSnapshot.Snapshot.__contains__(stock_code):
                # mod = all_list.loc[ix].to_json()
                # RealTimeSnapshot.factory.broadcast(mod)
                resultdict[stock_code] = mod
                RealTimeSnapshot.Snapshot[stock_code] = all_list.loc[ix]

            else:
                snapshot_data = RealTimeSnapshot.Snapshot[stock_code]
                # 判断收到的行情价、量是否与快照中的价、量相同。不相同则记录，相同则丢弃
                if snapshot_data['volume'] != all_list.at[ix, 'volume'] \
                        or snapshot_data['price'] != all_list.at[ix, 'price'] \
                        or snapshot_data['amount'] != all_list.at[ix, 'amount']:
                    # print(all_list.loc[ix].to_json())
                    # 发送事件

                    # mod = all_list.loc[ix].to_json()
                    # RealTimeSnapshot.factory.broadcast(mod)
                    resultdict[stock_code] = mod
                    RealTimeSnapshot.Snapshot[stock_code] = all_list.loc[ix]

                    if isrecorddata:
                        filename = dataPath + stock_code + '.csv'
                        if os.path.exists(filename):
                            # 文件存在，则追加单行数据
                            all_list.iloc[ix:ix + 1, :].to_csv(filename, mode="a", header=None)
                        else:
                            # 文件不存在，则记录头和数据
                            all_list.iloc[ix:ix + 1, :].to_csv(filename)

                    # print(str(snapshot_data['volume']) + ";" + str(v['volume']))

        except Exception as e:
            logging.error(e)

    # print(json.dumps(resultdict))
    return resultdict;


def sendListDatabylistEasyquotation(codelist):
    '''
    根据代码列表获取行情并发送数据 行情源：easyquotation
    :param codelist: 代码列表
    :return:
    '''
    global RealTimeSnapshot, StockNoList
    all_list_code = []
    all_list_code = codelist
    resultdict = {}

    all_list = quotation.stocks(all_list_code)

    for (k, v) in all_list.items():

        try:
            stock_code = k
            v['code'] = k
            # mod = json.dumps(v, ensure_ascii=True)
            mod = str(v)
            # resultdict[stock_code] = mod
            #factory.broadcast(mod)
            if not RealTimeSnapshot.Snapshot.__contains__(stock_code):

                resultdict[stock_code] = mod
                RealTimeSnapshot.Snapshot[stock_code] = v
                # print(v)
            else:
                snapshot_data = RealTimeSnapshot.Snapshot[stock_code]
                # print(k + "  " + str(v['name']) + "  " + str(v['now']))
                # 判断收到的行情价、量是否与快照中的价、量相同。不相同则记录，相同则丢弃
                if snapshot_data['volume'] != v['volume'] \
                        or snapshot_data['now'] != v['now'] \
                        or snapshot_data['turnover'] != v['turnover']:
                    # 发送事件
                    # RealTimeSnapshot.factory.broadcast(mod)
                    resultdict[stock_code] = mod
                    RealTimeSnapshot.Snapshot[stock_code] = v

                    if isrecorddata:

                        df = pandas.DataFrame.from_dict(v, orient='index').T

                        filename = dataPath + stock_code + '.csv'
                        if os.path.exists(filename):
                            # 文件存在，则追加单行数据
                            df.to_csv(filename, mode="a", header=None)
                        else:
                            # 文件不存在，则记录头和数据
                            df.to_csv(filename, encoding='utf_8_sig')

        except Exception as e:
            logging.error(e)

    return resultdict;


def senddatabylistEasyquotation(codelist):
    '''
    根据代码列表获取行情并发送数据 行情源：easyquotation
    :param codelist: 代码列表
    :return:
    '''
    global RealTimeSnapshot

    all_list_code = codelist

    all_list = quotation.stocks(all_list_code)

    for (k, v) in all_list.items():

        try:
            stock_code = k
            # mod = json.dumps(v, ensure_ascii=True)
            v['code'] = k
            mod = str(v)
            if not RealTimeSnapshot.Snapshot.__contains__(stock_code):

                RealTimeSnapshot.factory.broadcast(mod)
                RealTimeSnapshot.Snapshot[stock_code] = v

            else:
                snapshot_data = RealTimeSnapshot.Snapshot[stock_code]
                # print(k + "  " + str(v['name']) + "  " + str(v['now']))
                # 判断收到的行情价、量是否与快照中的价、量相同。不相同则记录，相同则丢弃
                if snapshot_data['volume'] != v['volume'] \
                        or snapshot_data['now'] != v['now'] \
                        or snapshot_data['turnover'] != v['turnover']:
                    # 发送事件
                    RealTimeSnapshot.factory.broadcast(mod)
                    RealTimeSnapshot.Snapshot[stock_code] = v

                    if isrecorddata:
                        '''# 将数据保存到csv文件
                        filename = stock_code + '.json'
                        fr = open(os.path.join(dataPath, filename), 'a')
                        # mod = json.dumps(v)
                        print(mod)
                        fr.write(mod + ",\n")
                        fr.close()'''
                        # df = pandas.Series(v, index=v.keys)
                        df = pandas.DataFrame.from_dict(v, orient='index').T

                        filename = dataPath + stock_code + '.csv'
                        if os.path.exists(filename):
                            # 文件存在，则追加单行数据
                            df.to_csv(filename, mode="a", header=None)
                        else:
                            # 文件不存在，则记录头和数据
                            df.to_csv(filename, encoding='utf_8_sig')

        except Exception as e:
            logging.error(e)


def senddatabylistTushare(codelist):
    '''
    根据代码列表获取行情并发送数据 行情源：tushare
    :param codelist: 代码列表
    :return:
    '''
    global RealTimeSnapshot
    all_list_code = codelist
    all_list = ts.get_realtime_quotes(all_list_code)

    for ix in all_list.index:

        stock_code = all_list.loc[ix]['code']
        mod = all_list.loc[ix].to_json()

        if not RealTimeSnapshot.Snapshot.__contains__(stock_code):
            RealTimeSnapshot.factory.broadcast(mod)
            RealTimeSnapshot.Snapshot[stock_code] = all_list.loc[ix]

        else:
            snapshot_data = RealTimeSnapshot.Snapshot[stock_code]
            # 判断收到的行情价、量是否与快照中的价、量相同。不相同则记录，相同则丢弃
            if snapshot_data['volume'] != all_list.at[ix, 'volume'] \
                    or snapshot_data['price'] != all_list.at[ix, 'price'] \
                    or snapshot_data['amount'] != all_list.at[ix, 'amount']:
                # 发送事件
                RealTimeSnapshot.factory.broadcast(mod)
                RealTimeSnapshot.Snapshot[stock_code] = all_list.loc[ix]

                if isrecorddata:
                    filename = dataPath + stock_code + '.csv'
                    if os.path.exists(filename):
                        # 文件存在，则追加单行数据
                        all_list.iloc[ix:ix + 1, :].to_csv(filename, mode="a", header=None)
                    else:
                        # 文件不存在，则记录头和数据
                        all_list.iloc[ix:ix + 1, :].to_csv(filename)

                # print(str(snapshot_data['volume']) + ";" + str(v['volume']))


def readyGo():
    '''
    执行操作
    '''
    # 交易所行情快照
    global RealTimeSnapshot, dataPath, StockNoList

    # 获取代码
    getChangeCode()

    # 初始化行情快照
    if datasource == "easyquotation":
        iniSnapshotByeasyquotation()
    elif datasource == "tushare":
        iniSnapshotBytushare()

    if isrecorddata:
        # 记录行情数据
        try:
            curNow = datetime.datetime.now()
            curY = str(curNow.year)
            curM = str(curNow.month).zfill(2)
            curD = str(curNow.now().day).zfill(2)

            dataPath = dataPath + curY + curM + curD + '/' + change + '/'

            if not os.path.exists(dataPath):
                os.makedirs(dataPath)
        except Exception as ex:
            logging.error(ex,)


    # 计算行情分组
    sum_stock = len(StockNoList)
    sum_mod = sum_stock % querymaxcodes
    I = sum_stock // querymaxcodes
    if sum_mod > 0:
        I = I + 1

    # 记录分组行情代码
    threadcodes = []
    for i in range(0, I, 1):
        if (i + 1) * querymaxcodes > sum_stock:
            all_list = StockNoList[i * querymaxcodes: sum_stock]
            threadcodes.append(all_list)
            # print(i * querymaxcodes, sum_stock - 1)
        else:
            all_list = StockNoList[i * querymaxcodes: (i + 1) * querymaxcodes]
            # print(i * querymaxcodes, (i + 1) * querymaxcodes - 1)
            threadcodes.append(all_list)

    # 开启websocket服务
    websocketstart()

    # 开始循环查行情，发送数据
    while True:
        try:
            if ismultthread:
                # 多线程接收行情,处理发送数据
                with Pool(max_workers=I) as executor:
                    # 开启分组线程处理
                    if datasource == "easyquotation":
                        # easyquotation行情源
                        if issendList:
                            # 发送行情列表数据
                            future_tasks = {executor.submit(sendListDatabylistEasyquotation, sublist): sublist for sublist in threadcodes}
                            # senddatabylistQUO(StockNoList)
                            for future in concurrent.futures.as_completed(future_tasks):
                                # task = future_tasks[future]
                                try:
                                    # 返回处理行情列表
                                    data = future.result()
                                    if len(data) > 0:
                                        # 返回数据不为空
                                        mod = json.dumps(data)
                                        # print(len(data))
                                        RealTimeSnapshot.factory.broadcast(mod)
                                except Exception as exc:
                                    if islog:
                                        logging.error(exc)
                                    else:
                                        pass
                                # else:
                                #     print('%r page is %d bytes' % (task, len(data)))
                        else:
                            # 发送单个行情数据
                            future_tasks = [executor.submit(senddatabylistEasyquotation, sublist) for sublist in
                                           threadcodes]

                    elif datasource == "tushare":
                        if issendList:
                            # 发送行情列表
                            # name = input()
                            future_tasks = {executor.submit(sendListDatabylistTushare, sublist): sublist for sublist in threadcodes}
                            for future in concurrent.futures.as_completed(future_tasks):
                                # task = future_tasks[future]
                                try:
                                    data = future.result()
                                    if len(data) > 0:
                                        mod = json.dumps(data)
                                        # print(len(data))
                                        RealTimeSnapshot.factory.broadcast(mod)
                                except Exception as exc:
                                    if islog:
                                        logging.error(exc)
                                    else:
                                        pass
                        else:
                            future_tasks = [executor.submit(senddatabylistTushare, sublist) for sublist in
                                            threadcodes]
            else:
                # 单线程处理行情数据
                if datasource == "easyquotation":
                    [senddatabylistEasyquotation(sublist) for sublist in threadcodes]
                    # senddatabylistQUO(StockNoList)
                elif datasource == "tushare":
                    [senddatabylistTushare(sublist) for sublist in threadcodes]

            #end1 = datetime.datetime.now()
            # print(str((end1 - end).seconds))
            #logging.info("for:" + str((end1 - end).microseconds))
        except Exception as e:
            # pass
            logging.error(e)
            # break
            # finally:


if __name__ == "__main__":
    # getChangeCode()
    readyGo()
