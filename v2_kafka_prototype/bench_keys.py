import json
import numpy as np
from kafka import KafkaProducer, KafkaConsumer
from cy_utils3 import cos
from redis import StrictRedis
import zlib
r = StrictRedis.from_url('redis://10.0.0.10:6379')
pipe = r.pipeline()
consumer = KafkaConsumer('keys', bootstrap_servers='10.0.0.11:9092')
c2 = KafkaConsumer('features', bootstrap_servers='10.0.0.11:9092')
data_store = np.zeros((300,2),dtype=np.int32)

#for f in c2:
#    target = json.loads(f.value.decode('utf-8'))
#    t_inds = target['i']
#    t_vals = target['v']
#print(t_inds, t_vals)
#producer = KafkaProducer(bootstrap_servers='10.0.0.8:9092')
#producer.send('foobar',b'new message')
t_vals = np.zeros(100).astype(np.float64)
t_inds = np.zeros(100).astype(np.int32)
import time
st = time.time()
count = 0
total = 0
for msg in consumer:
    count += 1
    total += len(json.loads(msg.value.decode('utf-8'))['k'].split(','))
    if count%100 == 0:
        print(count, total, time.time()-st, count/(time.time()-st))
#    print(json.loads(msg.value.decode('utf-8')))
#    msgs = json.loads(msg.value.decode('utf-8'))
    #print(keys)
#    keys = msgs['k'].split(',')
#    print(msgs['i'])
#    target_inds = np.fromstring(msgs['i'], dtype=int, sep=' ').astype(np.int32)
#    target_vals = np.fromstring(msgs['v'], dtype=float, sep=' ')
#    target_inds = np.frombuffer(zlib.decompress(bytes(keys['i'])))
#    target_vals = np.frombuffer(zlib.decompress(bytes(keys['v'])))
#    print(len(keys))
#    scores_list = []
    # collect all the keys
#    for key in keys:
#        key_front = key[:-1]
#        key_back = key[-1:]
#        pipe.hget(key_front, key_back+':i')
#        pipe.hget(key_front, key_back+':v')
        # pull the data, decompress it, and change the data type
#    values = pipe.execute()
#    inds = values[::2]
#    vals = values[1::2]
#    scores = [(0.0,'blank')]
#    data_store = np.zeros((300,2),dtype=np.int32)
#    max = 0.0
#    for ind, val, key in zip(inds, vals, keys):
#        ind = np.array(np.frombuffer(zlib.decompress(ind),dtype=np.int32))
#        val = np.random.rand(150)
#        val = np.frombuffer(zlib.decompress(val),dtype=np.float16).astype(np.float64)
#        ind = np.random.randint(0,104857,size=len(val)).astype(np.int32)
#        sv = SparseVector(1048576, ind, val)
#        sc = sv.dot(compare)/(compare.norm(2)*sv.norm(2))
        #sc = np.random.rand(1)[0]
#        sc = cos(ind, val, target_inds, target_vals, data_store)
#        sc = cos(ind, val, target_inds, target_vals, data_store)
#        if sc > max(scores)[0] or len(scores) < 5:
#            scores.append((sc, key))
        
#        scores.append((cos(ind, val, target_inds, target_vals),key))
#    print(len(scores))
#    scores = sorted(scores, reverse=True)
#    print(scores[:5])
    # write the top 5
#    dd = dict()
#    for score, key in scores[:5]:
#        print(score, key)
#        pipe.zadd('temp0', {key:score})
#    pipe.execute()




        #sc = cos(target_inds, target_vals, 
#for key in keys:
    
    #vals = keys['v']
#    sc = cos(t_inds, t_vals, inds, vals, data_store)
#    print(len(keys))
#    print(type(msg))
#    print(dir(msg))
#    print(msg[4])
#    keys = json.loads(msg[6].decode('utf-8'))
#    print(len(keys))
#keys = json.loads(msg[1].decode('utf-8'))
#    print(len(keys))
#    keys = msg[0]['k'].split(",")
#    print(len(keys))
#    print(msg)
#    for _ in range(100):
#        producer.send('foobar', b'some_message_bytes')

