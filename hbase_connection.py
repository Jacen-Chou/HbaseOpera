from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

from hbase import Hbase
from hbase.ttypes import *

from hbase_demo import create_table

def connection():
    # transport = TSocket.TSocket('af86c6bf0c65', 9090)
    transport = TSocket.TSocket('202.204.62.145', 9090)

    transport = TTransport.TBufferedTransport(transport)
    protocol = TBinaryProtocol.TBinaryProtocol(transport)
    client = Hbase.Client(protocol)
    transport.open()

    create_table()

    return client