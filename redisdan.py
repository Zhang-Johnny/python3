import redis

def redis_node():
    node =redis.StrictRedis(host='127.0.0.1',port=6379,password='123456')
    node.set("name_test","aadmin")
    print (node.get("name_test"))

redis_node()