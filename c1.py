import pymongo
import csv

mongo_url = "127.0.0.1:27017"
DATABASE = "RulesDB1"
TABLE = "PaloRules1"

client = pymongo.MongoClient(mongo_url)
db_des = client[DATABASE]
db_des_table = db_des[TABLE]

with open(f"{DATABASE}_{TABLE}.csv", "w", newline='') as csvfileWriter:
    writer = csv.writer(csvfileWriter)
    
    fieldList = [
        "index",
        "name",
        "protocol",
        "source",
        "dest",
        "action",
        "conflictsList"
        ]

    innerList = [
        "ip",
        "port"
    ]

    writer.writerow(fieldList)

    allRecordRes = db_des_table.find({'source.ip':'60.212.128.1'})

    # write multiple records into csv file

    for record in allRecordRes:

        #print(f"record = {record}")
        recordValueLst = []
        for field in fieldList:
            if field not in record:
                recordValueLst.append("None")

		#write json data embedded
            elif (field == 'source'):
                print ('found soure')
                source_results = record[field]

                for i in innerList:
                    print ('source reulst ,  i ', source_results[i])
                    recordValueLst.append(source_results[i])
            elif (field == 'dest'):
                print ('~~~~~~found destination data ')
                dest_results = record[field]

                for i in innerList:
                    print ('dest  reulst ,  i ', dest_results[i])
                    recordValueLst.append(dest_results[i])
                
            else:
                recordValueLst.append(record[field])
        try:
            writer.writerow(recordValueLst)
        except Exception as e:
            print(f"write csv exception. e = {e}")
a = 1
print (f'====write==={a}')
