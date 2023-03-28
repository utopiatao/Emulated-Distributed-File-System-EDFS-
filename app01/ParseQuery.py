def parse_query(query):
    final_list = []  # final_list[0] stores 'select', 'from', and 'group by' [1] stores 'where'
    select_from = []  # if size of select_from is 2 that means no group by is in the query
    where_list = []

    query = query.lower()
    from_index = query.index('from')
    select = query[7:from_index - 1]
    select_from.append(select)

    if 'where' in query:
        where_index = query.index('where')
        from_table = query[from_index + 5: where_index - 1]
        select_from.append(from_table)
        while 'and' in query[where_index + 6:]:
            and_index = query[where_index + 6:].index('and')
            condition = query[where_index + 6: where_index + and_index + 5]
            where_list.append(condition)
            where_index = where_index + and_index + 4
        and_index = where_index
        if 'group by' in query:
            group_index = query.index('group by')
            semicol_index = query.index(';')
            where_list.append(query[and_index + 6: group_index - 1])
            select_from.append(query[group_index + 9: semicol_index])
        else:  # no group by
            semicol_index = query.index(';')
            where_list.append(query[and_index + 6: semicol_index])

    else:  # no where condition
        if 'group by' in query:
            group_index = query.index('group by')
            semicol_index = query.index(';')
            select_from.append(query[from_index + 5: group_index - 1])
            select_from.append(query[group_index + 9: semicol_index])
        else:  # no group by
            semicol_index = query.index(';')
            select_from.append(query[from_index + 5: semicol_index])

    final_list.append(select_from)
    final_list.append(where_list)
    return final_list


#s = "Select abc, dc from website, newpaper where 123 = 456 and hello = good and ni12=wo group by USC;"  # 测试使用
#print(parse_query(s))  # 测试使用
