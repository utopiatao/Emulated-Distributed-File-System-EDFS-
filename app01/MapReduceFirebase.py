from  app01.ParseQuery import parse_query
import requests
from datetime import date

def map_reduce_firebase(query):
    final_result = []
    room_type = []
    price_collect = []

    # query = "Select price, house id, room type from West Hollywood, CA where 370 <= price < 380 and starting date = 2022-12-16;"
    # query = "Select price, house id, room type from Redondo Beach, CA where 150 <= price <= 200 and starting date = 2022-10-12 and ending date = 2022-10-25;"
    string_list = parse_query(query)
    # print(string_list)
    selects = string_list[0][0].split(', ')
    requirements = string_list[1]
    start_date = None
    end_date = None
    duration = None
    location = string_list[0][1]
    lower_bound_price = None
    upper_bound_price = None

    for requirement in requirements:
        if 'price' in requirement:
            price_list = requirement.split('<')
            lower_bound_price = float(price_list[0])
            upper_bound_price = float(price_list[2][1:])
        if 'starting date' in requirement:
            if requirement[requirement.index('=') + 1] == ' ':
                start_date = requirement[requirement.index('=') + 2:]
            else:
                start_date = requirement[requirement.index('=') + 1:]
        if 'ending date' in requirement:
            if requirement[requirement.index('=') + 1] == ' ':
                end_date = requirement[requirement.index('=') + 2:]
            else:
                end_date = requirement[requirement.index('=') + 1:]
            # 计算时长：
            start_day = start_date.split("-")
            end_day = end_date.split("-")
            duration = (date(int(end_day[0]), int(end_day[1]), int(end_day[2])) - date(int(start_day[0]),
                                                                                       int(start_day[1]),
                                                                                       int(start_day[2]))).days - 1
    # 筛选符合条件的房源
    candidate = []
    for i in range(1, 16):
        if i < 10:
            url = "https://dsci551-1cacd-default-rtdb.firebaseio.com/dataNode/house_calendar_0" + str(i) + ".json"
        else:
            url = "https://dsci551-1cacd-default-rtdb.firebaseio.com/dataNode/house_calendar_" + str(i) + ".json"
        r = requests.get(url)
        r = r.json()
        for element in r:
            # match price
            price_boolean = False
            house_price = float(element["price"].replace(',', '')[1:])
            if lower_bound_price is not None:
                if upper_bound_price is not None:
                    if lower_bound_price < house_price < upper_bound_price:
                        price_boolean = True
                else:  # upper_bound_price == None
                    if house_price > lower_bound_price:
                        price_boolean = True
            else:  # lower_bound_price == None:
                if upper_bound_price is not None:
                    if house_price < upper_bound_price:
                        price_boolean = True
                else:  # lower and upper are both none
                    price_boolean = True

            # match duration
            duration_boolean = False
            if duration == None:
                duration_boolean = True
            else:
                if duration <= element['maximum_nights'] and duration >= element['minimum_nights']:
                    duration_boolean = True

            # match starting date
            start_date_boolean = False
            if element['date'] == start_date:
                start_date_boolean = True

            # append valid outcomes:
            if price_boolean and duration_boolean and start_date_boolean:
                candidate.append(element["listing_id"])
                price_collect.append(element["price"])

    # print(candidate)
    # Using house_info to match the location
    final_result_ids = []
    for i in range(1, 4):

        url = "https://dsci551-1cacd-default-rtdb.firebaseio.com/dataNode/house_info_0" + str(i) + ".json"
        r = requests.get(url)
        r = r.json()
        for element in r:
            if element["id"] in candidate:
                if element["host_location"].lower() == location:
                    final_result_ids.append(element['id'])
                    room_type.append(element["room_type"])
    # print("+"*100)
    # print(final_result_ids)

    # 收集select加入到final_result中
    for j in range(0, len(final_result_ids)):
        final_string = ''
        for i in range(0, len(candidate)):
            if final_result_ids[j] == candidate[i]:
                if 'price' in selects:
                    final_string = final_string + price_collect[i] + '|'
                if 'house id' in selects:
                    final_string = final_string + str(final_result_ids[j]) + '|'
                if 'room type' in selects:
                    final_string = final_string + room_type[j]
        if final_string != '':
            final_result.append(final_string)
    return final_result
    # print('=' * 100)
    # print(final_result)

if __name__ == '__main__':

    #query = "Select price, house id, room type from West Hollywood, CA where 370 <= price < 380 and starting date = 2022-12-16;"
    query = "Select price, house id, room type from Redondo Beach, CA where 150 <= price <= 200 and starting date = 2022-10-12 and ending date = 2022-10-25;"
    result = map_reduce_firebase(query)
    print(result)
