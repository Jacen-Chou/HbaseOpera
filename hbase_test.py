# -*- coding: utf-8 -*

from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

from hbase import Hbase
from hbase.ttypes import *

# 连接HBase
# transport = TSocket.TSocket('af86c6bf0c65', 9090)
transport = TSocket.TSocket('202.204.62.145', 9090)

transport = TTransport.TBufferedTransport(transport)
protocol = TBinaryProtocol.TBinaryProtocol(transport)
client = Hbase.Client(protocol)
transport.open()


# 创建表
contents = ColumnDescriptor(name='cf:', maxVersions=1)
client.disableTable('test')
client.deleteTable('test')
client.createTable('test2', [contents])

print(client.getTableNames())

# insert data
# transport.open()

row = 'row-key1'

mutations = [Mutation(column="cf:a", value="1")]
client.mutateRow('test2', row, mutations)

# get one row
tableName = 'test2'
rowKey = 'row-key1'

result = client.getRow(tableName, rowKey)
print(result)
for r in result:
    print('the row is ', r.row)
    print('the values is ', r.columns.get('cf:a').value)


