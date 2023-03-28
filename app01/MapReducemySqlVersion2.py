import MySQLdb
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, insert, select, and_, delete
import pandas as pd

def is_available(query):
    # 获取info file， calendar file， review file的partition名
    info_files = []
    calendar_files = []
    review_files = []
    for i in ['house_calendar', 'house_info', 'house_review']:
        try:
            mydb = MySQLdb.connect(host="192.168.10.102", database='dsci551_project', user="root", passwd="000000")
            tmp_query = "select table_name from DataNode where file_name = '" + i + "';"
            mycursor = mydb.cursor()
            mycursor.execute(tmp_query)
            a = mycursor.fetchall()
            # print(a)
        except Exception as e:
            mydb.close()
            return str(e)
        if i == 'house_calendar':
            for j in a:
                calendar_files.append(j[0])
        elif i == 'house_info':
            for j in a:
                info_files.append(j[0])
        else:
            for j in a:
                review_files.append(j[0])
    result = []
    if "review number" in query:
        for info_file in info_files:
            for review_file in review_files:
                sqlquery = "select I.id, I.price, I.host_location, I.room_type, count(*) from " + info_file + " as I join " + review_file + " as T on  I.id = T.listing_id group by I.id;"
                mycursor.execute(sqlquery)
                a = mycursor.fetchall()
                for b in a:
                    result.append(b)
    else:
        from_index = query.index('from')
        if 'where' in query:
            where_index = query.index('where')
            from_table = query[from_index + 5: where_index - 1]
        elif 'group by' in query:
            groupby_index = query.index('group by')
            from_table = query[from_index + 5: groupby_index - 1]
        else:
            semi_index = query.index(';')
            from_table = query[from_index + 5: semi_index - 1]
        if from_table in ['house_calendar', 'house_info', 'house_review']:
            parse_list = parse_query(query)
            if 'group by' in query:
                result = {}
                file_index = query.index(parse_list[1][0])
                file_length = len(parse_list[1][0])
                if (parse_list[1][0] == 'house_info'):
                    for file in info_files:
                        sqlquery = query[:file_index] + file + query[file_length + file_index:]
                        # print(sqlquery)
                        mycursor.execute(sqlquery)
                        a = mycursor.fetchall()
                        for b in a:  # reduce
                            if b[0] in result.keys():
                                result[b[0]] = result[b[0]] + b[1]
                            else:
                                result[b[0]] = b[1]
                if (parse_list[1][0] == 'house_calendar'):
                    for file in calendar_files:
                        sqlquery = query[:file_index] + file + query[file_length + file_index:]
                        # print(sqlquery)
                        try:
                            mycursor.execute(sqlquery)
                            a = mycursor.fetchall()
                            for b in a:  # reduce
                                if b[0] in result.keys():
                                    result[b[0]] = result[b[0]] + b[1]
                                else:
                                    result[b[0]] = b[1]
                        except Exception as e:
                            print(str(e))
                            break;

                result = list(result.items())
            else:
                for info in info_files:
                    for calendar in calendar_files:
                        where = ''
                        from_index = query.index('from')
                        select = query[:from_index]
                        if 'price' in select:
                            select = select.replace("price", info + ".price")
                        select += 'from '
                        select
                        if "where" in query:
                            final_passin_query = ''
                            has_lower_limit = False
                            has_upper_limit = False
                            from_index = query.index('from ')
                            where_index = query.index('where')
                            passin_query = query[0:from_index + 5] + 'house_info_01' + query[where_index - 1:]
                            where = 'where '
                            has_price = False
                            for i in parse_list[2]:
                                if 'price' in i:
                                    length_price = len(i)
                                    has_price = True
                                    price_string = i
                                else:
                                    where += i + " and "
                            where = where[:-5]
                            # 处理< price <
                            if has_price:
                                where += " and "
                                price_lst = price_string.split(' ')
                                has_lower_limit = False
                                has_upper_limit = False
                                if price_lst[0].isnumeric():
                                    has_lower_limit = True
                                    lower_limit = (price_lst[0])
                                if price_lst[-1].isnumeric():
                                    has_upper_limit = True
                                    upper_limit = (price_lst[-1])
                                if has_lower_limit:
                                    if has_upper_limit:
                                        where += lower_limit + "<=(substring(" + info + ".price, 2,10) + 0) and (substring(" + info + ".price, 2,10) + 0) <=" + upper_limit
                                    else:
                                        where += lower_limit + "<=(substring(" + info + ".price, 2,10) + 0)"
                                else:
                                    if has_upper_limit:
                                        where += "(substring(" + info + ".price, 2,10) + 0)<=" + upper_limit

                            where += ";"
                        try:
                            sqlquery = select + info + ' join ' + calendar + ' on ' + info + '.id = ' + calendar + '.listing_id ' + where
                            #print(sqlquery)

                            mycursor.execute(sqlquery)
                            a = mycursor.fetchall()
                            for b in a:
                                result.append(b)
                        except Exception as e:
                            return str(e)
        else:
            return "No such table. Please double check."

    # 去重
    set1 = set()
    for ret in result:
        set1.add(ret)
    result = list(set1)

    # order by
    try:
        if 'order by date' in query:
            result = Sort_Date(result)
        elif 'order by' in query:
            if 'desc' in query:
                result = Sort_Tuple2(result)
            else:
                result = Sort_Tuple(result)
    except:
        print('Something wrong with order by, plase check again')
    final_res = []
    for tuple in result:
        tmp = ""
        for element in tuple:
            tmp += (str(element) + '|')
        final_res.append(tmp[:-1])
    return final_res

def parse_query(query):
    select_lst = []
    from_lst = []

    where_lst = []
    groupby_lst = []
    has_where = False
    has_groupby = False
    key_words = ['where', 'group by']
    query = query.lower()
    # get关键词的index
    select_index = query.index('select')
    from_index = query.index('from')

    if 'where' in query:
        has_where = True
        where_index = query.index('where')
    if 'group by' in query:
        has_groupby = True
        groupby_index = query.index('group by')
    semicol_index = query.index(';')
    # get关键词的string
    select_string = query[select_index + 7: from_index - 1]
    if has_where:
        from_string = query[from_index + 5 : where_index - 1]
    elif has_groupby:
        from_string = query[from_index + 5 : groupby_index - 1]
    else:
        from_string = query[from_index + 5 :  -1]
    from_string
    if has_where:
        if has_groupby:
            where_string = query[where_index + 6:groupby_index - 1]
        else:
            where_string = query[where_index + 6: -1]
    if has_groupby:
        groupby_string = query[groupby_index + 9 : -1]
    # update lists
    select_lst = select_string.split(', ')
    from_lst.append(from_string)
    if has_where:
        where_lst = where_string.split(' and ')
    if has_groupby:
        groupby_lst.append(groupby_string)
    res=[]
    res.append(select_lst)
    res.append(from_lst)
    res.append(where_lst)
    res.append(groupby_lst)
    return res


def Sort_Tuple(tup):
    # getting length of list of tuples
    lst = len(tup)
    for i in range(0, lst):
        for j in range(0, lst - i - 1):
            if (tup[j][-1] >= tup[j + 1][-1]):
                temp = tup[j]
                tup[j] = tup[j + 1]
                tup[j + 1] = temp
    return tup
def Sort_Tuple2(tup):
    # getting length of list of tuples
    lst = len(tup)
    for i in range(0, lst):
        for j in range(0, lst - i - 1):
            if (tup[j][-1] < tup[j + 1][-1]):
                temp = tup[j]
                tup[j] = tup[j + 1]
                tup[j + 1] = temp
    return tup

def Sort_Date(tup):
    result = sorted(tup, key = lambda x : x[0])
    return result

