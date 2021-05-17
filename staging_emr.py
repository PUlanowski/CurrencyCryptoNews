from pyhive import hive
import thrift_sasl

conn = hive.Connection(host='ec2-3-235-79-220.compute-1.amazonaws.com',
                       port=10000,
                       username='admin')

cursor = conn.cursor()
cursor.execute("SELECT cool_stuff FROM hive_table")
for result in cursor.fetchall():
  use_result(result)