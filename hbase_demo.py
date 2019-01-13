# -*- coding: utf-8 -*

from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

from hbase import Hbase
from hbase.ttypes import *

# 查询所有表名
def get_table_names():
    # 打印所有表名
    print(client.getTableNames())
    # 打印每个表的信息
    for table_name in client.getTableNames():
        print(client.getColumnDescriptors(table_name))


# 创建表
def create_table(table_name, column):
    # 创建表
    client.createTable(table_name, [column])
    print('创建表' + table_name + '成功')


# 删除表
def delete_table(table_name):
    client.disableTable(table_name)
    client.deleteTable(table_name)
    print('删除表' + table_name + '成功')


# 插入数据
def insert_data(table_name, row_key, column, vlaue):
    mutations = [Mutation(column=column, value=vlaue)]
    client.mutateRow(table_name, row_key, mutations)
    print('插入数据成功')


# 查询数据
def get_data(table_name, row_key, column):
    result = client.get(table_name, row_key, column)
    print ('row_key' + '\t' + 'column' + '\t' + 'timestamp' + '\t' + 'value')
    print (row_key + '\t' + column + '\t' + str(result[0].timestamp) + '\t' + str(result[0].value))



if __name__ == '__main__':

    # 连接HBase
    # transport = TSocket.TSocket('af86c6bf0c65', 9090)
    transport = TSocket.TSocket('202.204.62.145', 9090)
    transport = TTransport.TBufferedTransport(transport)
    protocol = TBinaryProtocol.TBinaryProtocol(transport)
    client = Hbase.Client(protocol)
    transport.open()
    print('连接成功')

    # delete_table('test2')

    # 表名
    table_name = 'test'
    # # 定义列族
    # column = ColumnDescriptor(name='cf:', maxVersions=3)
    # # 创建表
    # create_table(table_name, column)
    # # 打印所以表名
    # get_table_names()

    row_key = 'row-key1'
    column= 'cf:a'
    # value= '1'
    # insert_data(table_name, row_key, column, value)

    get_data(table_name, row_key, column)




