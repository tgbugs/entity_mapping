import csv
import json
import matplotlib
import mappings
import numpy

"""
    TODO
    get a rough draft for website to work
    ask tom to varify org
"""

"""
    Organizes a csv file to extract data easily
"""

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
        self.provLabelsList = []
        self.provSynonymsList = []
        self.provCuratorList = []
        self.provAbbrevsList = []
        self.provSearchList = []
        self.provAcronymsList = []
        self.other = []
        self.provList = ['labels', 'synonyms', 'curator', 'abbrevs', 'search', 'acronyms', 'other']

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
        with open ('/Users/love/git/troy_entity_mapping/entity_mapping/' + newFileName + '.csv', 'w') as file:
            wr = csv.writer(file)
            for rows in raw_rows:
                wr.writerow(rows)
        print ("done making", newFileName+".csv file")

    def makeProvCsvRowList(self, row_number, prov_value):
        row = self.csv_row(row_number)
        if prov_value == "curator": #check point
            self.provCuratorList.append(row)
        elif prov_value == "labels":
            self.provLabelsList.append(row)
        elif prov_value == "abbrevs":
            self.provAbbrevsList.append(row)
        elif prov_value == "synonyms":
            self.provSynonymsList.append(row)
        elif prov_value == "search":
            self.provSearchList.append(row)
        elif prov_value == "acronyms":
            self.provAcronymsList.append(row)
        elif prov_value == "other":
            self.other.append(row)
        else:
            print("not a prov value")

    def makeProvCsv(self):
        for provTypes in self.provList:
            with open ('/Users/love/git/troy_entity_mapping/entity_mapping/' + provTypes + '.csv', 'w') as file:
                wr = csv.writer(file)
                if provTypes == "curator":  # check point
                    for rows in self.provCuratorList:
                        wr.writerow(rows)
                elif provTypes == "labels":  # check point
                    for rows in self.provLabelsList:
                        wr.writerow(rows)
                elif provTypes == "synonyms":  # check point
                    for rows in self.provSynonymsList:
                        wr.writerow(rows)
                elif provTypes == "abbrevs":  # check point
                    for rows in self.provAbbrevsList:
                        wr.writerow(rows)
                elif provTypes == "search":
                    for rows in self.provSearchList:
                        wr.writerow(rows)
                elif provTypes == "acronyms":
                    for rows in self.provAcronymsList:
                        wr.writerow(rows)
                else:
                    for rows in self.other:
                        wr.writerow(rows)
            print ("done making", provTypes+".csv file")

#source,table,column,value,input_value,candidate,identifier,fma_id,category,relation,prov,eid,ms,notes
book_nlx_154697_8_fma = csv_book('/Users/love/git/troy_entity_mapping/entity_mapping/mappings/nlx_154697_8_fma.csv')
#id,mapped_label,mapped_identifier
book_nif_0000_00508_5 = csv_book('/Users/love/git/troy_entity_mapping/entity_mapping/mappings/nif_0000_00508_5')
#
book_coco_uber_match = csv_book('/Users/love/git/troy_entity_mapping/entity_mapping/mappings/coco_uber_match.csv')
#
book_coco_uber_search = csv_book('/Users/love/git/troy_entity_mapping/entity_mapping/mappings/coco_uber_search.csv')
#
book_uberon_nervous = csv_book('/Users/love/git/troy_entity_mapping/entity_mapping/mappings/uberon-nervous')

eidList = []

"""main"""
for current_row_number in range(book_nlx_154697_8_fma.file_length):
    eid = book_nlx_154697_8_fma.cell_from_index(current_row_number, book_nlx_154697_8_fma.schema_location("eid"))
    prov = book_nlx_154697_8_fma.cell_from_index(current_row_number, book_nlx_154697_8_fma.schema_location("prov"))
    if eid != None and len(eid) > 1 :
        eidList.append(book_nlx_154697_8_fma.csv_row(current_row_number))
    else:
        book_nlx_154697_8_fma.makeProvCsvRowList(current_row_number, prov)

book_nlx_154697_8_fma.makeCsv('eids_exist', eidList)
book_nlx_154697_8_fma.makeProvCsv()

book_temp1 = csv_book('/Users/love/git/troy_entity_mapping/entity_mapping/eids_exist.csv')
book_temp2 = csv_book('/Users/love/git/troy_entity_mapping/entity_mapping/labels.csv')
book_temp3 = csv_book('/Users/love/git/troy_entity_mapping/entity_mapping/synonyms.csv')
book_temp4 = csv_book('/Users/love/git/troy_entity_mapping/entity_mapping/curator.csv')
book_temp5 = csv_book('/Users/love/git/troy_entity_mapping/entity_mapping/abbrevs.csv')
book_temp6 = csv_book('/Users/love/git/troy_entity_mapping/entity_mapping/search.csv')
book_temp7 = csv_book('/Users/love/git/troy_entity_mapping/entity_mapping/acronyms.csv')

print(book_nlx_154697_8_fma.file_length)
print(book_temp1.file_length)
print(book_temp2.file_length)
print(book_temp3.file_length)
print(book_temp4.file_length)
print(book_temp5.file_length)
print(book_temp6.file_length)
print(book_temp7.file_length)
print(book_temp2.file_length + book_temp3.file_length + book_temp4.file_length + book_temp5.file_length + book_temp6.file_length + book_temp7.file_length)
