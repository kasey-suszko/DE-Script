#Created By : Kasey Suszko
#Due Date: 02/05/2022

#ASSUMPTIONS MADE:
#1. Actions should be performed based on timestamp order, not status
#2. Records that are updated before being inserted should be inserted (like upsert - what I use at my current company)
#3. The order in which the records are printed does not matter
#4. A pretty output for this use case is better than a more concise one
#5. Tried to follow pep8 formatting https://www.python.org/dev/peps/pep-0008/

#THINGS I WOULD IMPROVE IF I HAD MORE TIME:
#1. Try not to use so many if statements & for loops
#2. Perform higher quality testing with more examples to check for edge cases
#3. Explore utilizing pandas dataframes and/or making a giant dict instead of lists of json objects
#4. Make count verification more accurate - if a record is upserted first and never updated again, my current print statements would be incorrect
#5. I would improve the while loop the program runs in to be formalized and easier to exit the program

# libraries
import json
import time

start_time = time.time()

def read_file(file):
    print('Reading Raw Data File....')
    #read file and append each field to a list
    raw_data = []
    with open(file) as f:
        for line in f:
            raw_data.append(json.loads(line))
    return(raw_data)

def actions(dict_list):
    print(len(dict_list), " : RECORD ACTIONS INGESTED")
    records = []
    insert_count = 0
    update_count = 0
    delete_count = 0
    upsert_count = 0

    #Iterate through data by action type, perform action
    #If record is updated before insert, I inserted that record like "upsert" functionality
    for dict_data in dict_list:
        if dict_data['action'] == 'INSERT':
            if dict_data['guid'] not in [item['guid'] for item in records]:
                records.append(dict_data)
                insert_count += 1
            else:
                for item in records:
                    if dict_data['guid'] == item['guid']:
                        item.update(dict_data)
                        insert_count += 1
        if dict_data['action'] == 'UPDATE':
            for item in records:
                if dict_data['guid'] == item['guid']:
                    item.update(dict_data)
                    update_count += 1
            if dict_data not in records:
                records.append(dict_data)
                upsert_count += 1
        if dict_data['action'] == 'DELETE':
            for item in records:
                if dict_data['guid'] == item['guid']:
                    records.remove(item)
                    delete_count += 1
        if dict_data['action'] not in ['INSERT', 'UPDATE', 'DELETE']:
            print('ACTION NOT VALID FOR RECORD : ', dict_data)

    #remove data fields that are not needed in final output
    for item in records:
        item.pop('source_table', None)
        item.pop('action', None)
        item.pop('timestamp', None)

    print(insert_count, ' : RECORDS INSERTED')
    print(update_count, ' : RECORDS UPDATED')
    print(delete_count, ' : RECORDS DELETED')
    print(upsert_count, ' : RECORDS UPDATE-INSERTED')
    print(insert_count - delete_count , ' : EXPECTED RECORDS IGNORING UPDATE-INSERT')
    print(len(records), ' : RECORDS IN DATABASE')
    print('--------------------------------------------')
    return records

def table_lists(raw_data):
    #sort raw data by timestamp value ascending
    try:
        raw_data.sort(key=lambda x: float(x["timestamp"]))
    except ValueError as e:
        raise

    company_list = []
    job_list = []
    position_list = []
    employee_list = []

    #iterate through json objects and sort by table
    try:
        for element in raw_data:
            if element['source_table'] == 'Company':
                company_list.append(element)
            if element['source_table'] == 'Job':
                job_list.append(element)
            if element['source_table'] == 'Position':
                position_list.append(element)
            if element['source_table'] == 'Employee':
                employee_list.append(element)
            elif element['source_table'] not in ['Company', 'Job', 'Position', 'Employee']:
                raise Exception('Source Table Not Valid :', element['source_table'])
    except ValueError as e:
        print('Caught this error: ' + repr(e))

    tables = [company_list, job_list, position_list, employee_list]
    return tables

def output(table_lists, raw_data):
    #import table lists
    company_list = table_lists(raw_data)[0]
    job_list = table_lists(raw_data)[1]
    position_list = table_lists(raw_data)[2]
    employee_list = table_lists(raw_data)[3]

    #could have used less code, but wanted pretty output
    print('Transforming Records....')
    print('--------------------------------------------')
    print('COMPANY')
    print('--------------------------------------------')
    for element in actions(company_list):
        print(element)
    print('--------------------------------------------')
    print('JOB')
    print('--------------------------------------------')
    for element in actions(job_list):
        print(element)
    print('--------------------------------------------')
    print('POSITION')
    print('--------------------------------------------')
    for element in actions(position_list):
        print(element)
    print('--------------------------------------------')
    print('EMPLOYEE')
    print('--------------------------------------------')
    for element in actions(employee_list):
        print(element)


#TESTS
def is_type_record(element):
    return "Company" == element['source_table']

def test_is_type_record():
    assert is_type_record(read_file(file)[1]) #input json object record such as read_file(file)[1]

def check_time_format(element):
    return type(float(element)) is float

def test_check_time_format():
    assert check_time_format('zoo') #input timestamp such as read_file(file)[1]['timestamp']

#INFORMAL TESTS - Check error handling in functions
#table_lists([{'source_table': 'Company', 'timestamp': 'zoo'}])
#table_lists([{'source_table': 'zoo', 'timestamp': '100'}])
#assert(len(records) == insertCount - deleteCount, "Record Counts Should Match")

if __name__ == "__main__":
    stop_program = 'RUN'
    while stop_program == 'RUN' :
        try:
            #input file - default is sample given
            file = str(input("Enter your file if not 'sample_payload.txt' or else press enter : ") or 'sample_payload.txt')
            #call function
            output(table_lists, read_file(file))
            print('--------------------------------------------')
            print("---completed in %s seconds ---" % (time.time() - start_time))
        except:
            print("Error : invalid input file ")
        stop_program = str(input("Enter any value but RUN to exit program or enter RUN to try again: "))





