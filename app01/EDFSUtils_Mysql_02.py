from app01.ErrMsgs import ErrMsgs
from app01.Constants import Constants

import pandas as pd
import sqlalchemy, math
from sqlalchemy_utils import database_exists, create_database
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, insert, select, and_, delete, or_
import yaml

class EDFSUtils_Mysql_02(object):
    engine = None
    @staticmethod
    def edfs_operation(command_input):
        command_input = EDFSUtils_Mysql_02.remove_space_and_slash(command_input)
        parts = command_input.split(' ')
        if len(parts) < 2:
            return ErrMsgs.COMMAND_LENGTH_ERROR
        command = parts[0]
        if Constants.EDFS_MKDIR == command:
            if len(parts) > 2:
                return ErrMsgs.COMMAND_LENGTH_ERROR
            return EDFSUtils_Mysql_02.mkdir(parts[1])
        elif Constants.EDFS_LS == command:
            if len(parts) > 2:
                return ErrMsgs.COMMAND_LENGTH_ERROR
            return EDFSUtils_Mysql_02.ls(parts[1])
        elif Constants.EDFS_CAT == command:
            if len(parts) > 2:
                return ErrMsgs.COMMAND_LENGTH_ERROR
            return EDFSUtils_Mysql_02.cat(parts[1])
        elif Constants.EDFS_PUT == command:
            if len(parts) != 4:
                return ErrMsgs.COMMAND_LENGTH_ERROR
            return EDFSUtils_Mysql_02.put(parts[1], parts[2], parts[3])
        elif Constants.EDFS_GET_PARTITION_LOCATIONS == command:
            if len(parts) > 2:
                return ErrMsgs.COMMAND_LENGTH_ERROR

            return EDFSUtils_Mysql_02.getPartitionLocations(parts[1])
        elif Constants.EDFS_READ_PARTITION == command:
            if len(parts) != 3:
                return ErrMsgs.COMMAND_LENGTH_ERROR
            return EDFSUtils_Mysql_02.readPartition(parts[1], parts[2])
        elif Constants.EDFS_RM == command:
            if len(parts) > 2:
                return ErrMsgs.COMMAND_LENGTH_ERROR
            return EDFSUtils_Mysql_02.rm(parts[1])
        else:
            ErrMsgs.COMMAND_NOT_FOUND

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
    def rm(dir_file):
        file_name = dir_file.split("/")[-1].split('.')[0]
        path = dir_file[:dir_file.rfind('/')]
        # runs a transaction
        with EDFSUtils_Mysql_02.engine.begin() as connection:
            metadata = MetaData()
            file_info = Table('DataNode', metadata, autoload=True, autoload_with=EDFSUtils_Mysql_02.engine)
            # query the partition tables name
            select_query = select([file_info.columns.table_name]).where(and_(file_info.columns.file_name == file_name,
                                                                             file_info.columns.file_path == path))
            partition_tables = connection.execute(select_query).fetchall()
            if len(partition_tables) == 0:
                return ErrMsgs.FILE_NOT_FOUND
            # drop partition tables
            for table in partition_tables:
                drop_query = "DROP TABLE IF EXISTS {};".format(table[0])
                connection.execute(drop_query)

            # delete file info from file_info table
            delete_query = delete(file_info).where(and_(file_info.columns.file_name == file_name,
                                                        file_info.columns.file_path == path))
            connection.execute(delete_query)

            return Constants.EDFS_REMOVE_FILE_SUCCESS
    @staticmethod
    def getPartitionLocations(path):
        file_path = path[:path.rfind('/')]
        file_name = path[path.rfind('/') + 1: path.rfind('.')]
        ret = []
        with EDFSUtils_Mysql_02.engine.begin() as connection:
            metadata = MetaData()
            tbl_info = Table('DataNode', metadata, autoload=True, autoload_with=EDFSUtils_Mysql_02.engine)
            select_query = select([tbl_info.columns.table_name,tbl_info.columns.file_path]).where(and_(tbl_info.columns.file_path == file_path,
                                                                            tbl_info.columns.file_name == file_name))
            query_result = connection.execute(select_query).fetchall()
            if len(query_result) > 0:
                ret = [item[1] + "/" + item[0] for item in query_result]
                return ret
        return ret

    @staticmethod
    def readPartition(path, partition_number):
        if (not partition_number.isnumeric()):
            return ErrMsgs.PARTITION_NUMBER_INVALID

        # file_path = path[:path.rfind('/')]
        partitionNumber = '_0' + partition_number if int(partition_number) < 10 else '_' + partition_number
        file_path = path[:path.rfind('/')]
        file_name = path[path.rfind('/') + 1: path.rfind('.')]
        table_name = path[path.rfind('/') + 1: path.rfind('.')] + partitionNumber
        ret = []
        with EDFSUtils_Mysql_02.engine.begin() as connection:
            metadata = MetaData()
            tbl_info = Table('DataNode', metadata, autoload=True, autoload_with=EDFSUtils_Mysql_02.engine)
            select_query = select([tbl_info.columns.table_name]).where(and_(tbl_info.columns.file_path == file_path,
                                                                            tbl_info.columns.file_name == file_name))

            query_result = connection.execute(select_query).fetchall()

            if len(query_result) == 0:
                return ErrMsgs.FILE_NOT_FOUND
            else:
                partition_numbers = [int(name[0].split("_")[2]) for name in query_result]
                partition_numbers.sort()

                partition_number2 = int(partition_number)
                upper_bound = partition_numbers[len(partition_numbers) - 1]
                lower_bound = partition_numbers[0]
                if (partition_number2 > upper_bound) or (partition_number2 < lower_bound):
                    return ErrMsgs.PARTITION_NUMBER_INVALID

            tbl_info = Table(table_name, metadata, autoload=True, autoload_with=EDFSUtils_Mysql_02.engine)
            select_query = select(tbl_info.columns)
            query_result = connection.execute(select_query).fetchall()

            for row in query_result:
                line = ""
                for item in row:
                    line = line + str(item) + " | "
                ret.append(line)
            return ret

    @staticmethod
    def split_file(file, directory, partition_k, connection):
        file_name = file.split('/')[-1].split('.')[0]
        # file_name = file.split('.')[0]
        # data = pd.read_csv(file, encoding="ISO-8859-1").fillna("Null")
        data = pd.read_csv(file, encoding="utf-8").fillna("Null")

        colnames = []
        for col in data.columns:
            if 'Unnamed' not in col:
                colnames.append(col)
        data = data[colnames]
        chunk = math.ceil(len(data) / partition_k)
        result = []
        start = 0
        stop = chunk

        for i in range(1, partition_k + 1):
            table_name = file_name + '_' + str(i).zfill(2)
            df = data.loc[start:stop - 1].reset_index(drop=True)
            df.columns = colnames
            df.to_sql(table_name, con=connection, index=False, if_exists='replace')
            result.append({'file_name': file_name, 'file_path': directory, 'table_name': table_name})
            start = stop
            stop = stop + chunk
        return result

    @staticmethod
    def put(file, directory, partition_k):
        k = int(partition_k)
        # runs a transaction
        with EDFSUtils_Mysql_02.engine.begin() as connection:
            EDFSUtils_Mysql_02.mkdir(directory)
            metadata = MetaData()
            metadata = MetaData()
            DataNode = Table('DataNode', metadata, autoload=True, autoload_with=connection)
            # split data and create partition data tables
            values = EDFSUtils_Mysql_02.split_file(file, directory, k, connection)
            # update DataNode table
            query = insert(DataNode)
            connection.execute(query, values)
        return Constants.PUT_SUCCESS

    @staticmethod
    def cat(path):
        ret = []
        file_path = path[:path.rfind('/')]
        with EDFSUtils_Mysql_02.engine.begin() as connection:
            metadata = MetaData()
            tbl_info = Table('NameNode', metadata, autoload=True, autoload_with=EDFSUtils_Mysql_02.engine)
            select_query = select([tbl_info.columns.id]).where(and_(tbl_info.columns.cur_path == file_path))
            query_result = connection.execute(select_query).fetchall()
            if len(query_result) == 0:
                return ErrMsgs.DR_NOT_FOUND
            file_name = path[path.rfind('/') + 1: path.rfind('.')]
            tbl_info = Table('DataNode', metadata, autoload=True, autoload_with=EDFSUtils_Mysql_02.engine)
            select_query = select([tbl_info.columns.table_name]).where(and_(tbl_info.columns.file_path == file_path,
                                                                            tbl_info.columns.file_name == file_name))
            query_result = connection.execute(select_query).fetchall()
            if len(query_result) == 0:
                return ErrMsgs.FILE_NOT_FOUND

            table_name = query_result[0][0]
            tbl_info = Table(table_name, metadata, autoload=True, autoload_with=EDFSUtils_Mysql_02.engine)
            select_query = select(tbl_info.columns)
            query_result = connection.execute(select_query).fetchall()
            col_list = [str(cname).split(".")[1] for cname in tbl_info.columns]
            col_names = "|".join(col_list)
            ret.append(col_names)

            for row in query_result:
                line = ""
                for item in row:
                    line = line + str(item) + "|"
                line = line[:-1]
                ret.append(line)
        return ret

    @staticmethod
    def show_table_info():
        ret = []
        ret.append('table name|fields')
        with EDFSUtils_Mysql_02.engine.begin() as connection:
            metadata = MetaData()
            tbl_info = Table('DataNode', metadata, autoload=True, autoload_with=EDFSUtils_Mysql_02.engine)
            select_query = select([tbl_info.columns.file_name])
            query_result = connection.execute(select_query).fetchall()
            table_names = set()
            for item in query_result:
                table_names.add(item[0])


            for name in table_names:
                metadata = MetaData()
                tbl_name = name + "_01"
                tbl_info = Table(tbl_name, metadata, autoload=True, autoload_with=EDFSUtils_Mysql_02.engine)

                col_list = [str(cname).split(".")[1] for cname in tbl_info.columns]
                col_list.insert(0, name)
                col_names = "|".join(col_list)
                ret.append(col_names)
        return ret

    @staticmethod
    def ls(input):
        ret = []
        with EDFSUtils_Mysql_02.engine.begin() as connection:
            metadata = MetaData()
            tbl_info = Table('NameNode', metadata, autoload=True, autoload_with=EDFSUtils_Mysql_02.engine)
            select_query = select([tbl_info.columns.cur_path]).where(or_(tbl_info.columns.parent_path == input,tbl_info.columns.cur_path == input))
            query_result = connection.execute(select_query).fetchall()
            if len(query_result) == 0:
                return ErrMsgs.DR_NOT_FOUND

            for item in query_result:
                subdir = item[0]
                if subdir != input:
                    ret.append(subdir[subdir.rfind('/') + 1:])

            metadata = MetaData()
            tbl_info = Table('DataNode', metadata, autoload=True, autoload_with=EDFSUtils_Mysql_02.engine)
            select_query = select([tbl_info.columns.table_name]).where(and_(tbl_info.columns.file_path == input))
            query_result = connection.execute(select_query).fetchall()

            for item in query_result:
                subdir = item[0]
                ret.append(subdir[subdir.rfind('/') + 1:])
        return ret

    @staticmethod
    def test_create():
        print("test_create")

    @staticmethod
    def create_database():
        f = open('app01/mysql_property.yaml')
        properties = yaml.load(f, Loader=yaml.FullLoader)
        user = properties['mysql_config']['user']
        password = properties['mysql_config']['password']
        host = properties['mysql_config']['host']
        port = int(properties['mysql_config']['port'])
        database = properties['mysql_config']['database']
        EDFSUtils_Mysql_02.engine = create_engine(url="mysql+pymysql://{0}:{1}@{2}:{3}/{4}".format(user, password, host, port, database),
                               encoding="utf8")

        if not database_exists(EDFSUtils_Mysql_02.engine.url):
            create_database(EDFSUtils_Mysql_02.engine.url)
        with EDFSUtils_Mysql_02.engine.begin() as connection:
            # If DataNode table don't exist, Create.
            if not sqlalchemy.inspect(connection).has_table('DataNode'):
                metadata = MetaData(connection)
                DataNode = Table('DataNode', metadata,
                                 Column('id', Integer, primary_key=True, nullable=False),
                                 Column('file_name', String(255)),
                                 Column('file_path', String(255)),
                                 Column('table_name', String(255)))
                metadata.create_all()
            if not sqlalchemy.inspect(connection).has_table('NameNode'):
                metadata = MetaData(connection)
                NameNode = Table('NameNode', metadata,
                                 Column('id', Integer, primary_key=True, nullable=False),
                                 Column('parent_path', String(255)),
                                 Column('cur_path', String(255)))
                metadata.create_all()

        print( "Databases inited" )


    @staticmethod
    def mkdir(input):
        with EDFSUtils_Mysql_02.engine.begin() as connection:
            metadata = MetaData()
            tbl_info = Table('NameNode', metadata, autoload=True, autoload_with=EDFSUtils_Mysql_02.engine)
            select_query = select([tbl_info.columns.id]).where(and_(tbl_info.columns.cur_path == input))

            query_result = connection.execute(select_query).fetchall()
            if len(query_result) != 0:
                return ErrMsgs.DIRECTORY_ALREADY_EXISTS

            parts = input.split('/')
            parent_path = "/"
            metadata = MetaData()
            tbl_info = Table('NameNode', metadata, autoload=True, autoload_with=EDFSUtils_Mysql_02.engine)
            for i in range(1, len(parts)):
                sub_path = parent_path + '/' + parts[i] if parent_path != '/' else '/' + parts[i]
                select_query = select([tbl_info.columns.id]).where(and_(tbl_info.columns.cur_path == sub_path))
                query_result = connection.execute(select_query).fetchall()
                if len(query_result) == 0:
                    query = insert(tbl_info)
                    connection.execute(query, {'parent_path': parent_path, 'cur_path': sub_path})
                parent_path = sub_path

        return Constants.MKDIR_SUCCESS
