def read_jdbc(spark,url,dbtable,user,password,driver="com.microsoft.sqlserver.jdbc.SQLServerDriver"):

    """  
    Read a table from realational database over jdbc

    url: connection url from source
    dbtable: table u want to ingest from source
    user: username od relational database
    password: password of relational database
    driver: driver from source
    
    """

    df = spark.read.format("jdbc") \
    .option("url",url  ) \
    .option("dbtable", dbtable) \
    .option("user", user) \
    .option("password", password) \
    .option("driver", driver) \
    .load()

    return df




def read_cosmosdb_json(spark,connection_string,database_name,collection_name):
    from pymongo import MongoClient
    from bson import json_util
    client = MongoClient(connection_string) 
    db = client[database_name] 
    collection = db[collection_name] 
    documents = list(collection.find({}))
    print(len(documents)) 
    json_rows = [] 
    for document in documents: 
        json_string = json_util.dumps(document) 
        json_rows.append((json_string,)) 
    df = spark.createDataFrame(json_rows, ["json_data"])
    return df




def read_s3_parquet(spark,aws_access_key_id ,aws_secret_access_key,bucket,folder,file):
   
    path = f"{bucket}/{folder}/{file}"
    source_path = (
        f"s3a://{aws_access_key_id}:{aws_secret_access_key}"
        f"@{path}" 
    )
    df=spark.read.parquet(source_path)
    return df




def write_raw(df,target_path,file_format,mode,options=None):
    writer= df.write.format(file_format).mode(mode)
    if options is not None:
        for key,value in (options or {}).items():
            writer.option(key,value)
    writer.save(target_path)
    return target_path
  










