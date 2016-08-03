import csv
import json
import matplotlib
import mappings
import numpy

"""
    X 1) curator level of prov with no eid match //organize
    X 2) search level of prov //actually search prov
    3) mapping exists (eid) OR single match for labels, syns, acros, abbrevs
    4) multiple match for labels, syns, acros, abbrevs
"""

"""
    Organizes a csv file to extract data easily
"""

folder = '/Users/love/git/troy_entity_mapping/'
class csv_book():

    def __init__(self, filename):
        # parses the csv file into a list of rows
        def open_with_csv(filename):
            with open(filename, 'r') as tsvin:
                data = []
                # delimiter for weird symbols
                tie_reader = csv.reader(tsvin)
                for line in tie_reader:
                    if line == "" or line == None or len(line) < 2:
                        pass
                    else:
                        data.append(line)
                return data
        # organizes cells into hash; [y][x]
        def data_dict():
            double_dict = {}
            single_dict = {}
            file_length = 0
            for row_location, row in list(enumerate(open_with_csv(filename))):
                file_length += 1
                single_dict[row_location] = row
                double_dict[row_location] = {}
                for column_location, cells in list(enumerate(row)):
                    double_dict[row_location][column_location]=cells
            return single_dict, double_dict, file_length

        self.filename = filename
        self.rows, self.data, self.file_length = data_dict()
        self.schema = {
            'source':0, 'table':1, 'column':2, 'value':2,  # loop variables
            'input_value':4, 'candidate':5, 'identifier':6, 'category':7, 'relation':8,
            'prov':9, 'eid':10, 'ms':11, 'notes':12 # eid => existing id,
        }
        self.provMultiList = []
        self.provSingleList = []
        self.provSearchList = []
        self.provNoEidList = []
        self.provList = ['no_eid', 'search', 'one_match_or_has_eid', 'multi_match']

    def csv_row(self, row_location):
        return self.rows[row_location]

    """just the header identifiers"""
    def schema_location(self, schemas):
        return self.schema[schemas] + 1

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
        with open (folder + 'entity_mapping/' + newFileName + '.csv', 'w') as file:
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

    def makeCsvFromList(self):
        for provTypes in self.provList:
            with open (folder + 'entity_mapping/' + provTypes + '.csv', 'w') as file:
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
            print ("done making", provTypes+".csv file")

#source,table,column,value,input_value,candidate,identifier,fma_id,category,relation,prov,eid,ms,notes
book_nlx_154697_8_fma = csv_book(folder + 'entity_mapping/mappings/nlx_154697_8_fma.csv')
#id,mapped_label,mapped_identifier
book_nif_0000_00508_5 = csv_book(folder + 'entity_mapping/mappings/nif_0000_00508_5')
#
book_coco_uber_match = csv_book(folder + 'entity_mapping/mappings/coco_uber_match.csv')
#
book_coco_uber_search = csv_book(folder + 'entity_mapping/mappings/coco_uber_search.csv')
#
book_uberon_nervous = csv_book(folder + 'entity_mapping/mappings/uberon-nervous')

noeidList = []
counter = 0

"""main"""
for current_row_number in range(book_nlx_154697_8_fma.file_length):
    eid = book_nlx_154697_8_fma.cell_from_index(current_row_number, book_nlx_154697_8_fma.schema_location("eid"))
    prov = book_nlx_154697_8_fma.cell_from_index(current_row_number, book_nlx_154697_8_fma.schema_location("prov"))
    if eid == None or len(eid) < 1:
        book_nlx_154697_8_fma.addToNoEidList(current_row_number)

    else:
        book_nlx_154697_8_fma.addToSearchList(current_row_number)
        source = book_nlx_154697_8_fma.schema_location("source")
        table = book_nlx_154697_8_fma.schema_location("table")
        column = book_nlx_154697_8_fma.schema_location("column")
        value = book_nlx_154697_8_fma.schema_location("value")
        crow = current_row_number

        if book_nlx_154697_8_fma.cell_from_index(crow, source) == book_nlx_154697_8_fma.cell_from_index(crow + 1, source):
            if book_nlx_154697_8_fma.cell_from_index(crow, table) == book_nlx_154697_8_fma.cell_from_index(crow + 1, table):
                if book_nlx_154697_8_fma.cell_from_index(crow, column) == book_nlx_154697_8_fma.cell_from_index(crow + 1, column):
                    #print (book_nlx_154697_8_fma.cell_from_index(crow, value).lower(), book_nlx_154697_8_fma.cell_from_index(crow + 1, value).lower())
                    if book_nlx_154697_8_fma.cell_from_index(crow, value).lower().strip() == book_nlx_154697_8_fma.cell_from_index(crow + 1, value).lower().strip():
                        book_nlx_154697_8_fma.addToMultiList(current_row_number)
                        counter += 1
                        #print (book_nlx_154697_8_fma.cell_from_index(crow, value).lower(), book_nlx_154697_8_fma.cell_from_index(crow + 1, value).lower())

        else:
            if counter == 0:
                print(book_nlx_154697_8_fma.cell_from_index(crow, value).lower(),
                      book_nlx_154697_8_fma.cell_from_index(crow + 1, value).lower())
                book_nlx_154697_8_fma.addToSingleList(current_row_number)
            else:
                book_nlx_154697_8_fma.addToMultiList(current_row_number)
                counter == 0

#book_nlx_154697_8_fma.printMultiMatch()
book_nlx_154697_8_fma.makeCsvFromList()

#book_temp1 = csv_book(folder + 'entity_mapping/no_eids.csv')
book_temp1 = csv_book(folder + 'entity_mapping/search.csv')
book_temp2 = csv_book(folder + 'entity_mapping/no_eid.csv')

print(book_temp1.file_length + book_temp2.file_length) # good :)
