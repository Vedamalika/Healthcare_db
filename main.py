from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.event import QueryEvent
from pymysqlreplication.row_event import (DeleteRowsEvent, UpdateRowsEvent, WriteRowsEvent)

def createBinlogStream(latest_binlog_state):
  return  BinLogStreamReader(
    connection_settings={"host":'localhost', "port":3306, "user":'root', "passwd":'Kunjara123@'},
    server_id = 1,
    blocking = True,
    resume_stream = True,
    log_file = latest_binlog_state.binlog_file,
    log_pos = latest_binlog_state.binlog_position,
    ignored_schemas = ['information_schema', 'performance_schema'], # Add your own here
    ignored_tables = ['alembic_version'],
    only_events = [DeleteRowsEvent, WriteRowsEvent, UpdateRowsEvent, QueryEvent]
  )

