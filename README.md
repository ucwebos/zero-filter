btree + RoaringBitmap + rocksdb + [soinc] 实现的简单筛选数据库 

    适用于数据量大 赛选条件值少的应用场景
    通过 soinc 可以部分代替es的简单功能
    内存占用小 (取决于赛选条件值的数量) 
    只提供query和put 两个API

 QUERY [/query] 语法
    类似 SQL select * from core where recId='1'
    {
        "bucket": "core",
        "where": [
            {
                "key": "recId",
                "op": "=",
                "val": "1"
            }
        ]
    }
    AND 类似 SQL select * from core where recId='1' and cateId>=1
    {
        "bucket": "core",
        "where": [
            {
                "key": "recId",
                "op": "=",
                "val": "1"
            },
            {
                "key": "cateId",
                "op": ">=",
                "val": "1"
            }
        ]
    }

    OR 类似 SQL select * from core where recId='1' or cateId>=1
    {
        "bucket": "core",
        "where": [
            {
                "or": [
                    {
                        "key": "recId",
                        "op": "=",
                        "val": "1"
                    },
                    {
                        "key": "cateId",
                        "op": ">=",
                        "val": "1"
                    }
                ]
            }
        ]
    }

    AND OR 组合 类似 SQL select * from core where recId=1 and (cateId='10' or cateId>=1)
    {
        "bucket": "core",
        "where": [
            {
                {
                    "key": "recId",
                    "op": "=",
                    "val": "1"
                }
            }
            {
                "or": [
                    {
                        "key": "cateId",
                        "op": "=",
                        "val": "10"
                    },
                    {
                        "key": "cateId",
                        "op": ">=",
                        "val": "1"
                    }
                ]
            }
        ]
    }


  PUT: [/put] 
    {
        "bucket": "core",
        "data": [
            {
                
                "appId": 0,
                "cateId": 22,
                "recId": "1",
                "userId": 277,
                "id": "600000000000000132"
            },
            {
                "appId": 4,
                "cateId": 49,
                "recId": "1",
                "userId": 221,
                "id": "600000000000008745"
            },
            {
                
                "appId": 0,
                "cateId": 22,
                "recId": "1",
                "userId": 277,
                "id": "600000000000001132"
            },
            {
                "appId": 4,
                "cateId": 49,
                "recId": "2",
                "userId": 221,
                "id": "600000000000018745"
            }
        ]
    }