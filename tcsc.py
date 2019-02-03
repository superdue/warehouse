# -*- coding:utf-8 –*-
'''
'''

import re
import shutil

import pandas
import pandas as pd
import os

# 订单号可用的列名称
order_column_pattern = '\*:订单号|订单编号|订单号'


# 建立单个文件的excel转换成csv函数,file 是excel文件名，to_file 是csv文件名。
def save_as_csv(file, to_file):
    data_xls = pd.read_excel(file)
    data_xls.to_csv(to_file, encoding='gb18030')


# 删除指定目录下所有匹配的文件
def rm_files(path, pattern):
    for root, dirs, files in os.walk(path, topdown=False):
        for name in files:
            match = re.search(pattern, name)
            if match:
                file_path = os.path.join(root, name)
                print("rm %s" % name)
                os.remove(file_path)


# 删除指定目录下所有不匹配的文件
def rm_files_except(path, pattern):
    for root, dirs, files in os.walk(path, topdown=False):
        for name in files:
            match = re.search(pattern, name)
            if match:
                pass
            else:
                file_path = os.path.join(root, name)
                print("rm %s" % name)
                os.remove(file_path)


# 批量重命名指定目录下的文件
def mv_preffix(path, prefix, newp):
    for root, dirs, files in os.walk(path, topdown=False):
        for name in files:
            match = re.search(prefix, name)
            if match:
                length = len(prefix)
                src_path = os.path.join(root, name)
                dest_path = os.path.join(root, newp + name[length:])
                print("mv %s to %s" % (src_path, dest_path))
                os.rename(src_path, dest_path)


# 将excel文件转换为csv文件
def excel_to_csv(source,dest=None):
    j = 1
    for root, dirs, files in os.walk(source, topdown=False):
        for name in files:
            file_path = os.path.join(root, name)
            filename = name.split('.')
            if dest:
                save_path = os.path.join(dest, "%s.csv" % str(j))
            else:
                save_path = os.path.join(root, "%s.csv" % str(j))
            if filename[-1] in ('xlsx', 'xls') and '~' not in filename[0]:
                print("save %s as csv " % name)
                # save_path = ob + '/' + str(j) + "_" + "".join(filename[0:-1]) + ".csv"
                # save_path = dest + '/excel_' + str(j) + ".csv"
                save_as_csv(file_path, save_path)
                j = j + 1
            else:
                shutil.copy(file_path, save_path)



# 打印指定目录下所有excel文件的第一行（列名）
def get_titles(file_dir):
    res = []
    for root, dirs, files in os.walk(file_dir, topdown=False):
        for col in files:
            filename = col.split('.')
            if filename[-1] in ['csv', 'xls', 'xlsx'] and '~' not in filename[0]:
                file_path = os.path.join(root, col)
                print(file_path)
                if filename[-1] == 'csv':
                    data = pandas.read_csv(file_path, encoding='gbk')
                elif filename[-1] in ['xls', 'xlsx']:
                    data = pandas.read_excel(file_path)
                res.append(str(list(data.columns)))
                print(list(data.columns))
    return res


# 获取所有发货数据的订单及供货商
def get_order_id_sender(file_dir):
    settle = pandas.DataFrame()
    for root, dirs, files in os.walk(file_dir, topdown=False):
        for col in files:
            filename = col.split('.')
            if 'no-send' not in filename and 'no-settle' not in filename \
                    and filename[-1] in ['csv', 'xls', 'xlsx'] and '~' not in filename[0]:
                file_path = os.path.join(root, col)
                print(file_path)
                if filename[-1] == 'csv':
                    data = pandas.read_csv(file_path, encoding='gbk')
                elif filename[-1] in ['xls', 'xlsx']:
                    data = pandas.read_excel(file_path)
                pattern = re.compile(order_column_pattern)
                res = ""
                for cname in data.columns:
                    order_id = re.search(pattern, cname)
                    if order_id:
                        res = order_id.group(0)
                        break
                    # j = cname.decode('gbk').find(u'订单号'.encode('gbk'))
                if res:
                    tmp = pandas.DataFrame()
                    tmp['orderid'] = data[res].astype(str).str.split(".", expand=True)[0].str.split("-", expand=True)[
                        0].str.strip()
                    tmp['sender'] = root.split("/")[-1]
                    print("tmp:len:%d" % len(tmp))
                    settle = settle.append(tmp)
    return settle


# 获取指定目录下的所有订单号
def get_order_id(file_dir):
    settle = pandas.Series()
    for root, dirs, files in os.walk(file_dir, topdown=False):
        for col in files:
            filename = col.split('.')
            if 'no-send' not in filename and 'no-settle' not in filename \
                    and filename[-1] in ['csv', 'xls', 'xlsx'] and '~' not in filename[0]:
                file_path = os.path.join(root, col)
                print(file_path)
                if filename[-1] == 'csv':
                    data = pandas.read_csv(file_path, encoding='gbk')
                elif filename[-1] in ['xls', 'xlsx']:
                    data = pandas.read_excel(file_path)
                pattern = re.compile(order_column_pattern)
                res = ""
                for cname in data.columns:
                    order_id = re.search(pattern, str(cname))
                    if order_id:
                        res = order_id.group(0)
                        break
                    # j = cname.decode('gbk').find(u'订单号'.encode('gbk'))
                if res:
                    tmp = data[res].astype(str).str.split(".", expand=True)[0].str.split("-", expand=True)[
                        0].str.strip()
                    print("tmp:len:%d" % len(tmp))
                    settle = settle.append(tmp)
                else:
                    print(".....not found order id in file[%s]" % file_path)
    return settle


# 匹配指定目录下的订单数据，将不匹配的订单数据保存在同级目录下面
def check_settle(settle, file_dir, save_flag):
    for root, dirs, files in os.walk(file_dir, topdown=False):
        for name in files:
            filename = name.split('.')
            if 'no-send' not in filename and 'no-settle' not in filename and '~' not in filename[0]:
                if filename[-1] in ['csv', 'xlsx', 'xls']:
                    file_path = os.path.join(root, name)
                    print("check file:%s" % file_path)
                    if filename[-1] == 'csv':
                        data = pandas.read_csv(file_path, encoding='gb18030')
                    elif filename[-1] in ['xls', 'xlsx']:
                        data = pandas.read_excel(file_path)
                    print("data len[%d]" % len(data))
                    if len(data) > 0:
                        pattern = re.compile(order_column_pattern)
                        res = ""
                        for cname in data.columns:
                            order_id = re.search(pattern, cname)
                            if order_id:
                                res = order_id.group(0)
                                break
                        # print(data.dtypes)
                        if res:
                            res_data = data[
                                -data[res].astype(str).str.split(".", expand=True)[0].str.split("-", expand=True)[
                                    0].str.strip().isin(settle)]
                            # print(res_data[0:5])
                            if len(res_data) > 0:
                                print("found no:%d,%d" % (len(res_data), len(data)))
                                save_path = os.path.join(root, "%s.%s" % (save_flag, name))
                                save_name, ext = os.path.splitext(save_path)
                                # print(save_path)
                                res_data.astype('str').to_excel(save_name + ".xls", encoding='gb18030', index=0)




# 拆分excel文件，单个文件30000行
def split_excel(file_path, sheet):
    path, ext = os.path.splitext(file_path)
    data = pandas.DataFrame()
    print(path, ext)
    if ext == '.csv':
        data = pandas.read_csv(file_path, encoding='gb18030', sheetname=sheet)
    elif ext in ['.xls', '.xlsx']:
        data = pandas.read_excel(file_path, sheetname=sheet)
    else:
        return
    length = len(data)
    i = 0
    print("len:%d" % length)
    while i < length:
        j = i + 30000
        j = min(length, j)
        data[i:j].astype('str').to_csv(path + str(i) + "sheet" + str(sheet) + ".csv", encoding='gb18030', index=0)
        i = j


# 赔付数据找对应的供货商
def find_send(sender, file_dir, save_flag):
    for root, dirs, files in os.walk(file_dir, topdown=False):
        for name in files:
            filename = name.split('.')
            if 'no-send' not in filename \
                    and 'no-settle' not in filename \
                    and 'with-sender' not in filename \
                    and '~' not in filename[0]:
                if filename[-1] in ['csv', 'xlsx', 'xls']:
                    file_path = os.path.join(root, name)
                    print("check file:%s" % file_path)
                    if filename[-1] == 'csv':
                        data = pandas.read_csv(file_path, encoding='gb18030', dtype=str)
                    elif filename[-1] in ['xls', 'xlsx']:
                        data = pandas.read_excel(file_path, dtype=str)
                    print("data len[%d]" % len(data))
                    if len(data) > 0:
                        pattern = re.compile(order_column_pattern)
                        res = ""
                        for cname in data.columns:
                            order_id = re.search(pattern, cname)
                            if order_id:
                                res = order_id.group(0)
                                break
                        # print(data.dtypes)
                        if res:
                            data['orderid'] = data[res].astype(str).str.split(".", expand=True)[0].str.strip()
                            # print(data['orderid'])
                            filter_data = data[(data['退款金额'].astype(float) > 0) | (data['赔款金额'].astype(float) > 0)]
                            new_data = pandas.merge(filter_data, sender, how='left', left_on='orderid',
                                                    right_on='orderid')
                            # new_data = pandas.merge(data, sender, how='left', left_on='orderid',
                            #                         right_on='orderid')
                            if len(new_data) > 0:
                                save_path = os.path.join(root, "%s.%s" % (save_flag, name))
                                save_name, ext = os.path.splitext(save_path)
                                print(save_path)
                                new_data.astype('str').to_excel(save_name + ".xls", encoding='gb18030', index=0)


# 主函数
def main():
    # 发货数据文件夹路径
    send_source = "/Users/snake/work/tcsc/数据处理/发货数据/原数据文件"
    # 如遇到excel无法处理，转csv处理
    # excel_to_csv(send_source)

    # 结算数据文件夹路径
    settle_source = "/Users/snake/work/tcsc/数据处理/结算数据/原数据文件"
    # 如遇到excel无法处理，转csv处理
    # excel_to_csv(settle_source)

    # 赔付数据文件夹路径
    other_source = "/Users/snake/work/tcsc/数据处理/赔付数据/原数据文件"
    # 如遇到excel无法处理，转csv处理
    # excel_to_csv(send_source,"/Users/snake/work/tcsc/数据处理/发货数据/csv")

    # 如有大文件，需拆分大文件，方便后续处理
    # split_excel(root_path + "/20181028-20181111-贝店账单-.xlsx",sheet=1)

    # 处理前最好检查所文件的列名，避免因表头原因产生的问题
    get_titles(send_source)
    # get_titles(settle_source)
    # get_titles(other_source)

    # 生成所有未结算的发货数据，文件前缀"no-settle"
    # rm_files(send_source, "no-settle")
    # settle = get_order_id(settle_source)
    # print("settle len:%d" % len(settle))
    # check_settle(settle=settle, file_dir=send_source, save_flag="no-settle")
    #
    # 检查无误后，将已结算的发货数据删除，保留未结算数据，作为下期结算待对账数据
    # rm_files_except(send_source, "no-settle")

    # 生成所有有结算没发货的数据，文件前缀"no-send"
    # rm_files(settle_source, "no-send")
    # send = get_order_id(send_source)
    # sets = send.drop_duplicates(keep='first')
    # print("send len:%d" % len(sets))
    # print(send)
    # check_settle(settle=sets, file_dir=settle_source, save_flag="no-send")

    # 生成赔付数据对应的供货商，文件前缀"with-sender"
    # rm_files(other_source, "with-sender")
    # sets = pandas.DataFrame()
    # sender = get_order_id_sender(send_source)
    # sets = sender.drop_duplicates(subset=['orderid'], keep='first')
    # print("sender len:%d" % len(sender))
    # print("sender len:%d" % len(sets))
    # find_send(sets, other_source, "with-sender")


if __name__ == '__main__':
    main()
