import sys
from rediscluster import StrictRedisCluster
import csv
#连接redis集群
def redis_cluster():
    redis_nodes =  [{'host':'123.59.198.68','port':8018},
                    {'host':'123.59.198.69','port':8018},
                    {'host':'123.59.198.70','port':8018},
                    {'host':'123.59.198.71','port':8018},
                    {'host':'123.59.198.89','port':8018},
                    {'host':'123.59.198.90','port':8018}
                    ]
    # redis_nodes =  [{'host':'192.168.10.101','port':8018},
    #                 {'host':'192.168.10.102','port':8018},
    #                 {'host':'192.168.10.103','port':8018},
    #                 {'host':'192.168.10.105','port':8018},
    #                 {'host':'192.168.10.108','port':8018},
    #                 {'host':'192.168.10.109','port':8018}
    #                 ]
    try:
        redisconn = StrictRedisCluster(startup_nodes=redis_nodes,decode_responses=True,password='xhx@2018')
    except Exception as e:
        print ("Connect Error!")
        sys.exit(1)
    saveRedis(redisconn)
    #print(redisconn.get('53278377'))
#组合数据
def combination(row,moduleDate):
    prpList=["id","companyId","juyuanId","entNameZh","entNameEn","regNo","socialCreditNo","orgNo","taxNo","country","keyNames","digest","updatedAt","createdAt","tmp1","tmp2"]
    for i in range(len(row)):
        if row[i]=='\t':
            moduleDate[prpList[i]]=''
        else:
            moduleDate[prpList[i]]=row[i]
#从文件读取数据，并录入redis
def saveRedis(redisconn):
    csv_file = 'C:/csv/index_data.csv' #CSV 文件的位置
    #csv_file = '/opt/fei_test/csv/index_data.csv'
    stringL={"regNo","taxNo","orgNo","socialCreditNo","companyId"}
    moduleDate={}
    #表字段顺序保持不变
    with open(csv_file, 'rt', encoding='utf-8') as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            combination(row,moduleDate)
            redisconn.set(moduleDate['id'],moduleDate)
            redisconn.set(moduleDate['entNameZh'],moduleDate['id'])
            for lin in stringL:
                if moduleDate[lin]:
                    if "companyId"==lin:
                        redisconn.set(lin+moduleDate[lin],moduleDate['id']+'@#$'+moduleDate['entNameZh'])
                    else:
                        redisconn.set(lin+moduleDate[lin],moduleDate['id'])
                print(moduleDate)

if __name__ == '__main__':
    redis_cluster()