import redis

redis_host = '10.0.0.7'
redis_port = 6379
redis_pasword = 'AhrIykRVjO9GHA52kmYou7iUrsDbzJL+/7vjeTYhsLmpskyAY8tnucf4QJ7FpvVzFNNKuIZVVkh1LRxF'

def hello_redis(n):
    """Example Hello Redis Program"""
   
    # step 3: create the Redis Connection object
    try:
   
        # The decode_repsonses flag here directs the client to convert the responses from Redis into Python strings
        # using the default encoding utf-8.  This is client specific.
        #r = redis.StrictRedis(host=redis_host, port=redis_port, password=redis_password, decode_responses=True)
        r = redis.Redis(host=redis_host, port=redis_port, password=redis_pasword)
        # step 4: Set the hello message in Redis
        r.set("t1", "1")

        # step 5: Retrieve the hello message from Redis
        msg = r.get("t1")
        for i in range(n):
            r.set("t1", msg+str(i))
            msg = r.get("t1")
        print(msg)        
   
    except Exception as e:
        print(e)

if __name__ == '__main__':
    hello_redis(10)
