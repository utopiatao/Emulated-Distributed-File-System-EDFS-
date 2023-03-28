import json

from django.shortcuts import render,HttpResponse
from app01.EDFSUtils_Mysql_02 import  EDFSUtils_Mysql_02
from app01.EDFSUtils_FireBase import  EDFSUtils
from app01.ErrMsgs import  ErrMsgs
from app01.Constants import Constants
# from app01.SqlParser import is_available
# Create your views here.
def index(request):
    return HttpResponse("hello world")

def edfs(request):
    return render(request, "hadoop.html")

def edfs_fb(request):
    return render(request, "hadoop_fb.html")

EDFSUtils_Mysql_02.create_database()

def showCurrentTable(request):
    results = EDFSUtils_Mysql_02.show_table_info()
    ret = dict()
    ret['dataset_display'] = 1
    if len(results) == 0:
        results = [Constants.EMPTY_TABLES]
        ret['dataset_display'] = 0
    ret['query_result'] = results
    return HttpResponse(json.dumps(ret), content_type='application/json')

from app01.MapReducemySqlVersion2 import is_available
def goClick(request):
    request_data = request.POST
    if request_data['input'] is None or len(request_data['input']) == 0:
        return HttpResponse(ErrMsgs.INPUT_ERROR)

    results = []
    if 'select' in request_data['input']:
        results = is_available(request_data['input'])
        ret = dict()
        ret['dataset_display'] = 1
        ret['query_result'] = results
        return HttpResponse(json.dumps(ret), content_type='application/json')
    else:
        results = EDFSUtils_Mysql_02.edfs_operation(request_data['input'])
        ret = dict()
        ret['dataset_display'] = 0
        if type(results) == str:
            results = [results]
        elif type(results) == list and len(results) == 0:
            results = [Constants.EMPTY_DIRECTORY]

        if Constants.EDFS_CAT in request_data['input'] or Constants.EDFS_READ_PARTITION in request_data['input']:
            ret['dataset_display'] = 1

        ret['query_result'] =  results
        return HttpResponse(json.dumps(ret), content_type='application/json')

from app01.MapReduceFirebase import map_reduce_firebase
def goClick_fb(request):
    request_data = request.POST
    if request_data['input'] is None or len(request_data['input']) == 0:
        return HttpResponse(ErrMsgs.INPUT_ERROR)

    results = []
    if 'select' in request_data['input']:
        results = is_available(request_data['input'])
        ret = dict()
        ret['dataset_display'] = 1
        ret['query_result'] = results
        return HttpResponse(json.dumps(ret), content_type='application/json')
    else:
        results = EDFSUtils.edfs_operation(request_data['input'])
        ret = dict()
        ret['dataset_display'] = 0
        if type(results) == str:
            results = [results]
        elif type(results) == list and len(results) == 0:
            results = [Constants.EMPTY_DIRECTORY]

        if Constants.EDFS_CAT in request_data['input'] or Constants.EDFS_READ_PARTITION in request_data['input']:
            ret['dataset_display'] = 1

        ret['query_result'] =  results
        return HttpResponse(json.dumps(ret), content_type='application/json')