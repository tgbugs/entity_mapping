import csv

"""
    X 1) curator level of prov with no eid match //organize
    X 2) search level of prov //actually search prov
    X 3) mapping exists (eid) OR single match for labels, syns, acros, abbrevs
    X 4) multiple match for labels, syns, acros, abbrevs
    5) some rows are out of order. make a second pass through the csv files created and pull out the remaining
        multi matches.
"""

"""
    Organizes a csv file to extract data easily
"""

folder = '/Users/love/git/troy_entity_mapping/'#entity_mapping/nlx_154697_8_fma.csv'
class csv_book():

    #def __init__(self, folder, filename):
        # parses the csv file into a list of rows
        #For when a csv is being uploaded
        #def open_with_csv(filename):
         #   with open(filename, 'r') as tsvin:
          #      data = []
                # delimiter for weird symbols
           #     tie_reader = csv.reader(tsvin)
            #    for line in tie_reader:
             #       if line == "" or line == None or len(line) < 2:
              #     else:
                #        data.append(line)
               # return data
        # organizes cells into hash; [y][x]
        # def data_dict(): //change back for csv uploading
    def __init__(self, folder, rows):

        def data_dict(rows):
            double_dict = {}
            single_dict = {}
            file_length = 0
            #for row_location, row in list(enumerate(open_with_csv(filename))):
            for row in rows:
                del row[6]
            for row_location, row in list(enumerate(rows)):
                file_length += 1
                single_dict[row_location] = row
                double_dict[row_location] = {}
                for column_location, cells in list(enumerate(row)):
                    double_dict[row_location][column_location]=cells
            return single_dict, double_dict, file_length

        self.folder = folder
        # self.filename = filename # for upload csv
        self.rows, self.data, self.file_length = data_dict(rows)
        self.schema = {
            'source':0, 'table':1, 'column':2, 'value':3,  # loop variables
            'input_value':4, 'candidate':5, 'identifier':6, 'category':7, 'relation':8,
            'prov':9, 'eid':10, 'ms':11, 'notes':12 # eid => existing id,
        }
        self.provMultiList = []
        self.provSingleList = []
        self.provSearchList = []
        self.provNoEidList = []
        self.demoList = []
        self.provList = ['no_eid', 'search', 'one_match_or_has_eid', 'multi_match']

    def csv_row(self, row_location):
        return self.rows[row_location]

    """just the header identifiers"""
    def schema_location(self, schemas):
        return self.schema[schemas]

    def schema(self):
        csv_schema = (
            'source', 'table', 'column', 'value',  # loop variables
            'input_value', 'candidate', 'identifier', 'category', 'relation',
            'prov', 'eid', 'ms', 'notes',  # eid => existing id,
        )
        return csv_schema

    def file_length(self):
        return self.file_length

    """grabs column info"""
    def column_info(self, location):
        try:
            mylist = [cells[location] for cells in self.data]
            return mylist
        except:
            print("location doesn't exist")

    def cell_from_index(self, row, schema_location):
        return self.data[row][schema_location]

    def makeCsv(self, newFileName,raw_rows):
        with open (self.folder + 'entity_mapping/' + newFileName + '.csv', 'w') as file:
            wr = csv.writer(file)
            for rows in raw_rows:
                wr.writerow(rows)
        print ("done making", newFileName+".csv file")

    def addToNoEidList(self, row_number):
        row = self.csv_row(row_number)
        self.provNoEidList.append(row)

    def printNoEidList(self):
        print(self.provNoEidList)

    def addToSearchList(self, row_number):
        row = self.csv_row(row_number)
        self.provSearchList.append(row)

    def addToSingleList(self, row_number):
        row = self.csv_row(row_number)
        self.provSingleList.append(row)

    def addToMultiList(self, row_number):
        row = self.csv_row(row_number)
        self.provMultiList.append(row)

    def printMultiMatch(self):
        print(self.provMultiList)

    def secondPassForSingle(self, value):
        for row_number, row in list(enumerate(self.provSingleList)):
            if value == row[self.schema_location("value")].lower().split():
                self.demoList.append(row)
                return row
        return None

    def demoExtra(self):
        dict_a = {}
        deleteList = []
        val_loc = self.schema_location("value")
        for rows in self.provSingleList:
            if rows[val_loc].lower() in dict_a:
                dict_a[rows[val_loc].lower()] += 1
            else:
                dict_a[rows[val_loc].lower()] = 0

        for rows in self.provSingleList:
            if dict_a[rows[val_loc].lower()] != 0:
                self.provMultiList.append(rows)
                deleteList.append(rows)
            else:
                pass

        for i in deleteList:
            if i in self.provSingleList:
                self.provSingleList.remove(i)

    def openMyCsvs(self, provType, id):
        path = self.folder + 'entity_mapping/' + provType + "_" + id + '.csv'
        fileLength = 0
        with open (path, 'r') as file:
            reader = csv.reader(file)
            rows = []
            for row in reader:
                if row == "" or row == None or len(row) < 1:
                    pass
                else:
                    fileLength += 1
                    rows.append(row)
        return rows, fileLength

    def makeCsvFromList(self, id):
        for provTypes in self.provList:
            path = self.folder + 'entity_mapping/' + provTypes + "_" + id + '.csv'
            #path = '/Users/love/Desktop/' + provTypes + "_" + id + '.csv'
            with open (path, 'w') as file:
                wr = csv.writer(file)
                if provTypes == 'search':
                    for rows in self.provSearchList:
                        wr.writerow(rows)
                elif provTypes == 'no_eid':
                    for rows in self.provNoEidList:
                        wr.writerow(rows)
                elif provTypes == 'one_match_or_has_eid':
                    for rows in self.provSingleList:
                        wr.writerow(rows)
                elif provTypes == 'multi_match':
                    for rows in self.provMultiList:
                        wr.writerow(rows)
                else:
                    print("Something went wrong making ", provTypes)
                    break
            #print ("done making", provTypes+".csv file")

    #tests the logic of the code
    def tester(self, id):
        for provTypes in self.provList:
            path = self.folder + 'entity_mapping/' + provTypes + "_" + id + '.csv'
            listChecker = []
            with open (path, 'r') as file:
                r = csv.reader(file)
                if provTypes == 'search':
                    for rows in self.provSearchList:
                        if rows[self.schema_location("eid")] == None:
                            print(provTypes, "is bad")
                            break
                        else : listChecker.append([rows[self.schema_location("eid")]])
                elif provTypes == 'no_eid':
                    for rows in self.provNoEidList:
                        if rows[self.schema_location("eid")] != "" :
                            print(provTypes, "is bad")
                            break
                        else : pass
                elif provTypes == 'one_match_or_has_eid':
                    dict_a = {}
                    val_loc = self.schema_location("value")
                    for rows in self.provSingleList:
                        if rows[val_loc].lower() in dict_a:
                            dict_a[rows[val_loc].lower()] += 1
                        else : dict_a[rows[val_loc].lower()] = 0
                    for rows in self.provSingleList:
                        if dict_a[rows[val_loc].lower()] != 0:
                            print(provTypes, "is bad and it stopped at", rows[val_loc].lower())
                            break
                        else : pass
                        #print(dict_a[rows[val_loc].lower()])
                elif provTypes == 'multi_match':
                    dict_a = {}
                    val_loc = self.schema_location("value")
                    for rows in self.provMultiList:
                        if rows[val_loc].lower() in dict_a:
                            dict_a[rows[val_loc].lower()] += 1
                        else : dict_a[rows[val_loc].lower()] = 0

                    for rows in self.provMultiList:
                        if dict_a[rows[val_loc].lower()] == 0:
                            print(provTypes, "is bad and it stopped at", rows[val_loc].lower())
                            break
                        else : pass
                else:
                    print("Something went wrong making csv for ", provTypes)
                    break
            #print (provTypes + "_" + id + '.csv is good!')




def makeSortedCSVFiles(path, rows):
    #THESE ARE FOR CSV FILE DIRECT UPLOADS
    #source,table,column,value,input_value,candidate,identifier,fma_id,category,relation,prov,eid,ms,notes
    #csvFile = csv_book(folder, folder + 'entity_mapping/mappings/' + csvFileName + '.csv') #For csv upload

    csvFile = csv_book(path, rows)

    """main"""
    for current_row_number in range(csvFile.file_length - 1):

        eid = csvFile.cell_from_index(current_row_number, csvFile.schema_location("eid"))

        if eid == None or len(eid) < 1:
            csvFile.addToNoEidList(current_row_number)

        else:
            csvFile.addToSearchList(current_row_number)
            value = csvFile.schema_location("value")
            crow = current_row_number

            try:
                if csvFile.cell_from_index(crow, value).lower().strip() == csvFile.cell_from_index(crow + 1, value).lower().strip():
                    csvFile.addToMultiList(current_row_number)
                    if "anterior amygdaloid area" == csvFile.cell_from_index(crow, value):
                        print("\n1\n")
                else:
                    if csvFile.cell_from_index(crow, value).lower().split() == csvFile.cell_from_index(crow - 1, value).lower().split():
                        csvFile.addToMultiList(current_row_number)
                        if "anterior amygdaloid area" == csvFile.cell_from_index(crow, value):
                            print("\n2\n")
                    else:
                        #test = csvFile.secondPassForSingle(csvFile.cell_from_index(crow, value).lower().split())
                        #if test != None:
                        #    csvFile.addToMultiList(current_row_number)
                        #    if "anterior amygdaloid area" == csvFile.cell_from_index(crow, value):
                        #        print("\n3\n")
                        #    csvFile.addToMultiList(test)
                        #else:
                        csvFile.addToSingleList(current_row_number)
                        #print(csvFile.cell_from_index(crow, value).lower().split(), csvFile.cell_from_index(crow + 1, value).lower().split())
            except:
                if csvFile.cell_from_index(crow, value).lower().strip() == csvFile.cell_from_index(crow + 1, value).lower().strip():
                    csvFile.addToMultiList(current_row_number)
                    if "anterior amygdaloid area" == csvFile.cell_from_index(crow, value):
                        print("\n4\n")
                else:
                    csvFile.addToSingleList(current_row_number)

    csvFile.demoExtra()
    csvFile.makeCsvFromList(csvFile.cell_from_index(1, csvFile.schema_location("source")))


    #length tester
    no_eid, no_eid_length = csvFile.openMyCsvs("no_eid", csvFile.cell_from_index(1, csvFile.schema_location("source")))
    search, search_length = csvFile.openMyCsvs("search", csvFile.cell_from_index(1, csvFile.schema_location("source")))
    one_match_or_has_eid, one_match_or_has_eid_length = csvFile.openMyCsvs("one_match_or_has_eid", csvFile.cell_from_index(1, csvFile.schema_location("source")))
    multi_match, multi_match_length = csvFile.openMyCsvs("multi_match", csvFile.cell_from_index(1, csvFile.schema_location("source")))

    #forther tests that aren't neccessary to show
    #print("checking lengths of no_eid + multi + single/ied vs ori... ", csvFile.file_length - 1, no_eid_length + multi_match_length + one_match_or_has_eid_length)
    #print("checking lengths of no_eid + search vs ori... ", csvFile.file_length - 1, no_eid_length + search_length) # good :)

    #logic tester
    csvFile.tester(csvFile.cell_from_index(1, csvFile.schema_location("source")))


"""
#For checking code locally
with open('/Users/love/git/troy_entity_mapping/entity_mapping/mappings/nlx_154697_8_fma.csv', 'r') as file:
    reader = csv.reader(file)
    rows = []
    for row in reader:
        if row  == "" or row == None or len(row) < 1:
            pass
        else:
            rows.append(row)

makeSortedCSVFiles(folder, rows)
"""
