import cProfile
from redis import StrictRedis
r = StrictRedis.from_url('redis://10.0.0.10:6379')
import time
to_run = """for i in range(10000):
#    r = StrictRedis.from_url('redis://10.0.0.10:6379')
    r.get('test')
#    time.sleep(.0001)
"""

cProfile.run(to_run)

