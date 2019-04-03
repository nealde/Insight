import redis

redis_host = '10.0.0.7'
redis_port = 6379
redis_password = 'AhrIykRVjO9GHA52kmYou7iUrsDbzJL+/7vjeTYhsLmpskyAY8tnucf4QJ7FpvVzFNNKuIZVVkh1LRxF'
r = redis.Redis(host=redis_host, port=redis_port, password=redis_password)

