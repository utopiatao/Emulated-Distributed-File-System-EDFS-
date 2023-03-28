from app01.ErrMsgs import  ErrMsgs
from app01.Constants import Constants
import requests
import json
import pandas as pd
import math

class EDFSUtils(object):
    @staticmethod
    def query_firebase(url):
        response = requests.get(url)
        search_ret = response.json()
        return search_ret

    @staticmethod
    def search_content_to_string(search_ret):
        ret = []
        for partition_location in search_ret.keys():
            url2 = search_ret[partition_location]
            partition_data = EDFSUtils.query_firebase(url2)
            for row in partition_data:
                ret.append(EDFSUtils.dict_to_string(row))
        return ret

    @staticmethod
    def cat(path):
        path = EDFSUtils.check_path(path)
        path = path.split('.')[0]
        url = 'https://dsci551-5af46-default-rtdb.firebaseio.com/nameNode/root/%s.json'%path
        search_ret = EDFSUtils.query_firebase(url)
        if search_ret is None:
            return ErrMsgs.FILE_NOT_FOUND
        ret =  EDFSUtils.search_content_to_string(search_ret)
        return ret

    @staticmethod
    def dict_to_string(row_data):
        temp = ""
        for key in row_data.keys():
            temp += str(row_data[key])
            temp += " | "
        return temp[:-1]

    @staticmethod
    def insert_firebase(url, content):
        response = requests.put(url, content)
        status_code = response.status_code
        return status_code

    @staticmethod
    def check_path(path):
        if  path.index('/') == 0:
            path = path[1:]
        if path[-1]  == '/':
            path = path[:-1]
        return path

    @staticmethod
    def delete_firebase(url):
        response = requests.delete(url)
        status_code = response.status_code
        return status_code

    # @staticmethod
    # def put(filePath, path, k):
    #     path = EDFSUtils.check_path(path)
    #     fileName = filePath.split("/")[-1].split('.')[0]
    #     url = 'https://dsci551-5af46-default-rtdb.firebaseio.com/filePath/%s.json' %fileName
    #     parent_directory = 'https://dsci551-5af46-default-rtdb.firebaseio.com/nameNode/root/%s/%s/.json' %(path, fileName)
    #     content = {'parent_directory': parent_directory}
    #     status_code = EDFSUtils.insert_firebase(url, json.dumps(content))
    #     if status_code != 200:
    #         return ErrMsgs.EDFS_PUT_FAILED
    #     return Constants.PUT_SUCCESS
        #call function

    @staticmethod
    def getPartitionLocations(filePath):
        filePath = EDFSUtils.check_path(filePath).split(".")[0]
        url =  'https://dsci551-5af46-default-rtdb.firebaseio.com/nameNode/root/%s.json' % filePath
        search_ret = EDFSUtils.query_firebase(url)
        if search_ret is None:
            return ErrMsgs.GET_PARTITION_LOCATIONS_FAILED
        ret = []
        for partitionID in search_ret.keys():
            ret.append("/" + filePath + "/" + partitionID)
        return ret

    @staticmethod
    def readPartition(filePath, partitionNumber):
        if (not partitionNumber.isnumeric()):
            return ErrMsgs.PARTITION_NUMBER_INVALID

        filePath = EDFSUtils.check_path(filePath).split('.')[0]
        partitionNumber = 'p0' + partitionNumber if int(partitionNumber) < 10 else 'p' + partitionNumber
        url = 'https://dsci551-5af46-default-rtdb.firebaseio.com/nameNode/root/%s/%s.json' % (filePath,partitionNumber)
        search_ret = EDFSUtils.query_firebase(url)
        if search_ret is None:
            return ErrMsgs.PARTITION_DOES_NOT_EXIST

        url = search_ret
        search_ret = EDFSUtils.query_firebase(url)
        ret = []
        for row in search_ret:
            ret.append(EDFSUtils.dict_to_string(row))
        # ret = EDFSUtils.search_content_to_string(search_ret)
        return ret

    @staticmethod
    def ls(path):
        ret = []
        path = EDFSUtils.check_path(path)
        url = 'https://dsci551-5af46-default-rtdb.firebaseio.com/nameNode/root/%s/.json' % path
        search_ret = EDFSUtils.query_firebase(url)
        if search_ret is None:
            return ErrMsgs.DR_NOT_FOUND

        for item in search_ret.keys():
            if item != Constants.INIT_DIR:
                ret.append(item)

        return ret


    @staticmethod
    def mkdir(path):
        path = EDFSUtils.check_path(path)
        # url = 'https://dsci551-5af46-default-rtdb.firebaseio.com/directory/%s/.json' % path
        url = 'https://dsci551-5af46-default-rtdb.firebaseio.com/nameNode/root/%s.json' % path
        search_ret = EDFSUtils.query_firebase(url)
        if search_ret is None:
            content = {Constants.INIT_DIR : -1 }
            status_code1 = EDFSUtils.insert_firebase(url, json.dumps(content))
            if status_code1 != 200:
                return ErrMsgs.MKDIR_FAILED
            return Constants.MKDIR_SUCCESS
        else:
            return ErrMsgs.DIRECTORY_ALREADY_EXISTS

    @staticmethod
    def split_file(file, partition_size, dataNode_url):
        path_url = dataNode_url +  '.json'
        search_ret = EDFSUtils.query_firebase(path_url)
        if search_ret is None:
            content = {Constants.INIT_DIR: -1}
            status_code1 = EDFSUtils.insert_firebase(path_url, json.dumps(content))
        file_name = file.split('.')[0].split('/')[-1]
        data = pd.read_csv(file).fillna("Null")
        chunk = math.ceil(len(data) / partition_size)
        partition_result = {}
        start = 0
        stop = chunk
        for i in range(1, partition_size + 1):
            partition = {}
            df = data.loc[start:stop - 1].reset_index(drop=True)
            file_url = dataNode_url + file_name + '_' + str(i).zfill(2) + '.json'
            key = 'p' + str(i).zfill(2)
            partition_result[key] = file_url
            for index, row in df.iterrows():
                ob = {}
                ob_key = index
                ob.update(row)
                partition[ob_key] = ob
            response = requests.put(file_url, json.dumps(partition))
            status_code = response.status_code
            stop = stop + chunk
        return partition_result

    @staticmethod
    def put(file, directory, partition_size):
        partition_size = int(partition_size)
        file_name = file.split('.')[0].split('/')[-1]
        url = 'https://dsci551-5af46-default-rtdb.firebaseio.com/'

        dataNode_url = url + 'dataNode/'
        partition_result = EDFSUtils.split_file(file, partition_size, dataNode_url)
        namenode_url = url + 'nameNode/root'
        path_url =  namenode_url + directory + '.json'
        search_ret = EDFSUtils.query_firebase(path_url)
        if search_ret is None:
            status_code = EDFSUtils.mkdir(directory)
        dir_url = namenode_url + directory + '/' + file_name + '.json'
        response = requests.put(dir_url, json.dumps(partition_result))
        status_code = response.status_code
        if status_code != 200:
            return ErrMsgs.PUT_FILE_FAILED
        else:
            return Constants.PUT_SUCCESS

    @staticmethod
    def rm(dir_file):
        stop = dir_file.rfind('/')
        file_name = dir_file.split('.')[0]
        path = dir_file[:stop]
        url = 'https://dsci551-5af46-default-rtdb.firebaseio.com/'
        path_url = url + 'nameNode/root' + path + '.json'
        file_url = url + 'nameNode/root' + file_name + '.json'
        dataNode_url = url + 'dataNode.json'

        path_ob = list(requests.get(path_url).json().keys())
        dataNode_ob = len(list(requests.get(dataNode_url).json().keys()))
        partitions = requests.get(file_url).json()
        partition_k = len(partitions)

        # remove partition data from dataNode
        for partition_url in partitions.values():
            requests.delete(partition_url)

        # keep maintain namenode and datanode, avoid removing nodes from firebase when they are empty.
        # if dataNode_ob - partition_k == 0:
        #     requests.put(dataNode_url, json.dumps(""))

        status_code = 200
        if len(path_ob) > 1:
            response = requests.delete(file_url)
            status_code = response.status_code
        else:
            response = requests.delete(file_url)
            # response = requests.put(path_url, json.dumps(""))
            status_code = response.status_code
        if status_code == 200:
            return Constants.RM_SUCCESS
        return Constants.RM_FAILED

    @staticmethod
    def remove_space_and_slash(command_input):
        ret = []
        for i in range(len(command_input)):
            if command_input[i] == ' ' and (i == 0 or i == len(command_input) - 1):
                continue;
            elif command_input[i] == ' ' and (i > 0 and command_input[i - 1] != ' '):
                ret.append(command_input[i])
            elif command_input[i] == '\\':
                ret.append('/')
            elif command_input[i] != ' ':
                ret.append(command_input[i])
        if ret[len(ret) - 1] == ' ' or ret[len(ret) - 1] == '/':
            ret = ret[:-1]
        return "".join(ret)

    @staticmethod
    def edfs_operation(command_input):
        command_input = EDFSUtils.remove_space_and_slash(command_input)
        parts = command_input.split(' ')
        if len(parts) < 2:
            return ErrMsgs.COMMAND_LENGTH_ERROR
        command = parts[0]
        if Constants.EDFS_MKDIR == command:
            if len(parts) > 2:
                return ErrMsgs.COMMAND_LENGTH_ERROR
            return EDFSUtils.mkdir(parts[1])
        elif Constants.EDFS_LS == command:
            if len(parts) > 2:
                return ErrMsgs.COMMAND_LENGTH_ERROR
            return EDFSUtils.ls(parts[1])
        elif Constants.EDFS_CAT == command:
            if len(parts) > 2:
                return ErrMsgs.COMMAND_LENGTH_ERROR
            return EDFSUtils.cat(parts[1])
        elif Constants.EDFS_PUT == command:
            if len(parts) != 4:
                return ErrMsgs.COMMAND_LENGTH_ERROR
            return EDFSUtils.put(parts[1], parts[2], parts[3])
        elif Constants.EDFS_GET_PARTITION_LOCATIONS == command:
            if len(parts) > 2:
                return ErrMsgs.COMMAND_LENGTH_ERROR
            return EDFSUtils.getPartitionLocations(parts[1])
        elif Constants.EDFS_READ_PARTITION == command:
            if len(parts) != 3:
                return ErrMsgs.COMMAND_LENGTH_ERROR
            return EDFSUtils.readPartition(parts[1],parts[2])
        elif Constants.EDFS_RM == command:
            if len(parts) > 2:
                return ErrMsgs.COMMAND_LENGTH_ERROR
            return EDFSUtils.rm(parts[1])
        else:
            ErrMsgs.COMMAND_NOT_FOUND