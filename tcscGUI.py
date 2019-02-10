import os
import re
import tkinter
import tkinter as tk
from tkinter import scrolledtext
from tkinter.filedialog import askdirectory
import tkinter.messagebox

import pandas

window = tk.Tk()
window.title('结算数据对账')
window.geometry('650x600')

send_source = tk.StringVar()
settle_source = tk.StringVar()
compensate_source = tk.StringVar()

send_match1 = tk.StringVar()
send_match1.set('\*:订单号|订单编号|订单号')
send_match2 = tk.StringVar()
settle_match1 = tk.StringVar()
settle_match2 = tk.StringVar()
compensate_match1 = tk.StringVar()
compensate_match2 = tk.StringVar()

scrolW = 90  # 设置文本框的长度
scrolH = 20  # 设置文本框的高度
scr = scrolledtext.ScrolledText(window, width=scrolW, height=scrolH,
                                wrap=tk.WORD)  # wrap=tk.WORD   这个值表示在行的末尾如果有一个单词跨行，会将该单词放到下一行显示,比如输入hello，he在第一行的行尾,llo在第二行的行首, 这时如果wrap=tk.WORD，则表示会将 hello 这个单词挪到下一行行首显示, wrap默认的值为tk.CHAR
scr.grid(row=10, column=0, columnspan=12)  # columnspan 个人理解是将3列合并成一列


def set_send_souce():
    path_ = askdirectory()
    send_source.set(path_)


def set_settle_source():
    path_ = askdirectory()
    settle_source.set(path_)


def set_compensate_source():
    path_ = askdirectory()
    compensate_source.set(path_)


def clear_log():
    scr.delete(1.0, tk.END)


def mylog(value):
    scr.insert(tk.END, value)
    scr.insert(tk.END, '\n')
    scr.see(tk.END)


def print_path():
    clear_log()
    scr.insert(tk.END, send_source.get() + "\n")
    scr.insert(tk.END, settle_source.get() + "\n")
    scr.insert(tk.END, compensate_source.get() + "\n")
    scr.see(tk.END)


tk.Label(window, text="发货数据目录：").grid(row=0, column=0, columnspan=1)
tk.Entry(window, textvariable=send_source, width=50).grid(row=0, column=1, columnspan=4)
tk.Button(window, text="选择", command=set_send_souce).grid(row=0, column=5)
tk.Label(window, text="匹配列1:").grid(row=1, column=0)
tk.Entry(window, textvariable=send_match1, width=50).grid(row=1, column=1, columnspan=4)
tk.Label(window, text="匹配列2:").grid(row=2, column=0)
tk.Entry(window, textvariable=send_match2, width=50).grid(row=2, column=1, columnspan=4)

tk.Label(window, text="结算数据目录：").grid(row=3, column=0, columnspan=1)
tk.Entry(window, textvariable=settle_source, width=50).grid(row=3, column=1, columnspan=4)
tk.Button(window, text="选择", command=set_send_souce).grid(row=3, column=5)
tk.Label(window, text="匹配列1:").grid(row=4, column=0)
tk.Entry(window, textvariable=settle_match1, width=50).grid(row=4, column=1, columnspan=4)
tk.Label(window, text="匹配列2:").grid(row=5, column=0)
tk.Entry(window, textvariable=settle_match2, width=50).grid(row=5, column=1, columnspan=4)

tk.Label(window, text="赔付数据目录：").grid(row=6, column=0, columnspan=1)
tk.Entry(window, textvariable=compensate_source, width=50).grid(row=6, column=1, columnspan=4)
tk.Button(window, text="选择", command=set_send_souce).grid(row=6, column=5)
tk.Label(window, text="匹配列1:").grid(row=7, column=0)
tk.Entry(window, textvariable=compensate_match1, width=50).grid(row=7, column=1, columnspan=4)
tk.Label(window, text="匹配列2:").grid(row=8, column=0)
tk.Entry(window, textvariable=compensate_match2, width=50).grid(row=8, column=1, columnspan=4)


# 删除指定目录下所有匹配的文件
def rm_files(path, pattern):
    for root, dirs, files in os.walk(path, topdown=False):
        for name in files:
            match = re.search(pattern, name)
            if match:
                file_path = os.path.join(root, name)
                mylog("rm %s" % name)
                os.remove(file_path)


def get_data_frame(file_dir, **matches):
    res_data = pandas.DataFrame()
    mylog(file_dir)
    for root, dirs, files in os.walk(file_dir, topdown=False):
        for col in files:
            filename = col.split('.')
            if 'no-send' not in filename and 'no-settle' not in filename \
                    and filename[-1] in ['csv', 'xls', 'xlsx'] and '~' not in filename[0]:
                file_path = os.path.join(root, col)
                mylog(file_path)
                if filename[-1] == 'csv':
                    data = pandas.read_csv(file_path, encoding='gbk')
                elif filename[-1] in ['xls', 'xlsx']:
                    data = pandas.read_excel(file_path)

                for column_name, patten in matches.items():
                    if len(patten) < 1:
                        break
                    pattern = re.compile(patten)
                    match_name = ""
                    for cname in data.columns:
                        matched = re.search(pattern, str(cname))
                        if matched:
                            match_name = matched.group(0)
                            break
                    # j = cname.decode('gbk').find(u'订单号'.encode('gbk'))
                    if match_name:
                        data[column_name] = \
                            data[match_name].astype(str).str.split(".", expand=True)[0].str.split("-", expand=True)[
                                0].str.strip()
                        res_data = res_data.append(data)
                    else:
                        flag = tkinter.messagebox.askyesno("warning",
                                                           "file[%s] not found match column ,continue？" % file_path)
                        if not flag:
                            exit(1)
    return res_data


def test():
    data = get_data_frame(send_source.get(), match1=send_match1.get(), match2=send_match2.get())
    mylog(str(len(data)))
    mylog(data[0:10])


def difference(left, right, on):
    """
    difference of two dataframes
    :param left: left dataframe
    :param right: right dataframe
    :param on: join key
    :return: difference dataframe
    """
    df = pandas.merge(left, right, how='left', on=on)
    left_columns = left.columns
    col_y = df.columns[left_columns.size]
    df = df[df[col_y].isnull()]
    df = df.ix[:, 0:left_columns.size]
    df.columns = left_columns
    return df


tk.Button(window, text='未发货订单', command=print_path).grid(row=9, column=1)
tk.Button(window, text='未结算订单', command=print_path).grid(row=9, column=2)
tk.Button(window, text='赔付订单', command=print_path).grid(row=9, column=3)
tk.Button(window, text='一键所有', command=test).grid(row=9, column=4)

window.mainloop()
