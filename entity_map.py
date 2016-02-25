#!/usr/bin/env python3
"""\
Usage:
    entity_map [ --help ]
    entity_map second third upload (<files>...)
    entity_map third upload (<files>...)
    entity_map make [--reup --remap] [<identifiers>...]
    entity_map second (<files>...)
    entity_map third (<files>...)
    entity_map upload [--apikeyfile=APIKEYFILE] (<files>...)
    entity_map clean (<files>...)

Options:
    --apikeyfile=APIKEYFILE     actually do the upload using the specified api key
    -r --reup                   redownload the data to be mapped to a local copy
    -m --remap                  redownload the mappings for external ids
"""
import csv
import json
from os import path
from collections import namedtuple
import requests
from docopt import docopt
from IPython import embed
from heatmaps.services import database_service
from pyontutils.scigraph_client import Refine, Vocabulary
#from pyontutils.scr_sync import mysql_conn_helper, create_engine, inspect
from exclude import exclude_table_prefixes, exclude_tables, exclude_columns

v = Vocabulary()#quiet=False)

class discodv(database_service):
    dbname = 'disco_crawler'
    user = 'discouser'
    host = 'nif-db.crbs.ucsd.edu'
    port = 5432
    DEBUG = True

csv_schema = (
    'source', 'table', 'column', 'value',  # loop variables
    'input_value', 'candidate', 'identifier', 'category', 'relation',
    'prov', 'eid', 'ms', 'notes',
)

valid_relations = ('exact', 'part of', 'subClassOf', 'located in')

prov_levels = {
    'spell':-2,
    None:-1,
    'labels':0,
    'synonyms':1,
    'abbrevs':2,
    'acronyms':3,
    'search':4,  # too much for initial, curator with identifier == None -> search
    'curator':5,
}

prov_order = [c for c in zip(*sorted([(l, p) for p, l in prov_levels.items() if l >= 0]))][1]

prov_functions = {
    'labels':(v.findByTerm, {'searchSynonyms':False}),
    'synonyms':(v.findByTerm, {}),
    'abbrevs':(v.findByTerm,
                     {'searchSynonyms':False,
                      'searchAbbreviations':True}),
    'acronyms':(v.findByTerm,
                {'searchSynonyms':False,
                 'searchAcronyms':True}),
    'search':(v.searchByTerm,
              {'searchSynonyms':True,
               #'searchAbbreviations':True,  # this kills the crab?
               #'searchAcronyms':True,  # this kills the crab?
               'limit':10}),
    #'curator':(lambda term, **args: [{'labels':[term],'curie':None}], {}),
    'curator':(lambda term, **args: None, {}),
}

external_id_map = {
    'l2_nlx_151885_data_summary':('n_name', 'nelx_id'),
    'l2_nlx_151885_data_neuron':('name', 'nelx_id'),
    'l2_nif_0000_37639_onto_label':('name', 'onto_id'),
}

column_category_map = {
    'brain_region':'anatomical entity',
    'species':'organism',
    'system_used':'Resource',
    'region1':'anatomical entity',
    'region2':'anatomical entity',
    'region3':'anatomical entity',
    'scientific_name':'organism',
}

def memoize(filepath, ser='json'):
    """ The wrapped function should take no arguments
        and return the object to be serialized
    """
    if ser == 'json':
        serialize = json.dump
        deserialize = json.load
        mode = 't'
    else:
        raise TypeError('Bad serialization format.')

    def inner(function):
        def superinner(reup=False, **kwargs):
            if path.exists(filepath) and not reup:
                print('deserializing from', filepath)
                with open(filepath, 'r' + mode) as f:
                    return deserialize(f)
            else:
                output = function(**kwargs)
                with open(filepath, 'w' + mode) as f:
                    serialize(output, f)
                return output

        return superinner

    return inner

@memoize('/home/tom/files/entity_mapping/ents.json')
def get_data(ids=None):
    data = {k:{} for k in ids}
    with discodv() as dv:
        SQL = "SELECT table_name FROM information_schema.tables WHERE table_schema='public'"
        #SQL = "select relname from pg_class where relkind='r' and relname !~ '^(pg_|sql_)';"
        table_names = dv.cursor_exec(SQL)
        for table_name, in table_names:
            SQL = "SELECT column_name from information_schema.columns WHERE table_name=%s"
            for id_ in ids:
                if id_ in table_name:
                    args = (table_name, )
                    columns = dv.cursor_exec(SQL, args)
                    print(columns)
                    data[id_][table_name] = {c:{} for c, in columns}

        for v in data.values():
            for table_name, columns in v.items():
                for column in columns:
                    SQL = "SELECT DISTINCT(%s) FROM %s" % (column, table_name)  # XXX DANGER ZONE
                    values = dv.cursor_exec(SQL)
                    #print(values)
                    columns[column] = [v if len(v) > 1 else v[0] for v in values]

        #print([v.keys() for v in data.values()])

        return data

@memoize('/home/tom/files/entity_mapping/mapping.json')
def get_mapping():
    data = {}  # put this here to avoid issues @ v in data.values() :(
    with discodv() as dv:
        for table_name, (name_col, id_col) in external_id_map.items():  # de ERD
            SQL = "SELECT DISTINCT(%s, %s) FROM %s" % (name_col, id_col, table_name)  # XXX DANGER ZONE
            mapping_ = dv.cursor_exec(SQL)
            mapping = {}
            for str_, in mapping_:
                str_ = str_.strip('(').rstrip(')')
                name, id_ = str_.rsplit(',', 1)  # stupid commas in names :/
                name = name.strip('"')
                mapping[name] = id_

            #print(mapping)
            data[table_name] = {  # FIXME may need to add support for various name columns :(
                'name_col':name_col,
                'id_col':id_col, 
                'map':mapping
            }

        return data

def refine(values):
    values = [str(v) if type(v) is not str else v for v in values]
    r = Refine()# oh yeah... this is why I implmented quiet in the first place quiet=False)
    queries = {q:{'query':q} for q in values}
    refined = r.suggestFromTerm_POST(queries=queries)
    #print('got it')
    refined = {k:[(d['name'], d['id'], d['type'][0] if d['type'] else '') for d in v['result']] if v['result'] else None for k, v in refined.items()}
    return [refined[k] for k in values]

def parse_notes(string):
    if string:
        vals = string.split(' ')
        if vals[0] == 'exp':
            return 0, vals[1]
        try:
            count = int(vals[0])
        except ValueError:
            return 1, None

        return count, tuple(vals[1:])  # XXX danger
    else:
        return 1, None

separators = (',', ';', '&', 'and ', ' or ', '|')
cart_seps = []  # unused now
for r in [[s1 + ' ' + s2 for s2 in separators] for s1 in separators]:
    cart_seps.extend(r)

def sep_all_the_things(value):
    seps = [sep for sep in separators if sep in value]
    vals = [value]
    if seps:
        for sep in seps:
            new_vals = []
            for val in vals:
                new_val = [v.strip() for v in val.split(sep)]
                new_vals.extend(new_val)
            vals = []
            for val in new_vals:
                if val:
                    val = val.replace('(','').replace(')','')
                    vals.append(val)
    return vals

def automated_dedupe(iv_candidate_identifier_cat_prov):
    """ operates in place and returns True if a dedupe occured
        recall that iv being none doesn't mean that the value
        isn't different between two runs with the same id pair
    """
    if len(iv_candidate_identifier_cat_prov) == 2:
        #print('automated_dedupe_', iv_candidate_identifier_cat_prov)
        #iv_candidate_identifier_cat_prov.sort(key=lambda r: r[2])  # apparently they come sorted
        #print('to dedupe', iv_candidate_identifier_cat_prov)
        first, second = [r[2] for r in iv_candidate_identifier_cat_prov]
        if first.startswith('NIFGA') and second.startswith('UBERON'):
            iv_candidate_identifier_cat_prov.pop(0)
        elif first.startswith('NCBITaxon') and second.startswith('NIFORG'):
            iv_candidate_identifier_cat_prov.pop()

        return True  # only return False if there wasn't a comparison

no_curies = set()
value_cache = {}  # TODO consider caching output values
def expand_map_value(column, value, split=False, existing_prov=None, skip=(), continue_=()):
    if value in value_cache:  # this really should never happen
        print('value cache hit')
        return value_cache[value]  # woo memoization

    iv_candidate_identifier_cat_prov = []

    SKIP_SPLIT = False
    split_values = sep_all_the_things(value)
    if not split and len(split_values) > 1:
        SKIP_SPLIT = True
        split_values = [value]


    for new_value in split_values:
        dedupe = True
        if new_value in value_cache:
            print('new_value cache hit')
            new_value_tups = value_cache[new_value]  # woo memoization
            iv_candidate_identifier_cat_prov.extend(new_value_tups)
            continue

        new_value_tups = []

        if SKIP_SPLIT:
            input_value = 'SKIP_SPLIT'  # used in 2nd pass
        elif new_value == value:
            input_value = None
        else:
            input_value = new_value

        previously_matched_ids = set()
        for prov in prov_order[prov_levels[existing_prov]+1:]:
            if prov in skip:
                continue
            function, kwargs = prov_functions[prov]
            records = function(new_value, **kwargs)  # match
            records = records if records else []

            processed_records = []
            for record in records:
                category = record['categories'][0] if record['categories'] else None  # FIXME
                if column in column_category_map and \
                   category != column_category_map[column]:
                    continue  # don't include results with category mismatch

                candidate = record['labels'][0]  # FIXME
                identifier = record['curie']
                if not identifier:
                    no_curies.add(candidate)  # subjectless labels for culling!
                    dedupe = False
                elif identifier not in previously_matched_ids:
                    previously_matched_ids.add(identifier)
                    processed_records.append((input_value, candidate, identifier, category, prov))

            if processed_records:
                if len(processed_records) > 1:
                    if automated_dedupe(processed_records):
                        dedupe = False
                new_value_tups.extend(processed_records)
                if prov not in continue_:
                    break  # we got a match at a high level don't need the rest atm

        if not new_value_tups:  # went through all the prov
            candidate = new_value
            identifier = None
            category = None
            new_value_tups.append((input_value, candidate, identifier, category, prov))
        elif dedupe:
            automated_dedupe(new_value_tups)

        iv_candidate_identifier_cat_prov.extend(new_value_tups)
        value_cache[new_value] = new_value_tups

    #return sorted(iv_candidate_identifier_prov)
    #return iv_candidate_identifier_prov.sort()
    value_cache[value] = iv_candidate_identifier_cat_prov
    return iv_candidate_identifier_cat_prov

def make_csvs(ids, reup=False, remap=False):
    print(ids)
    data = get_data(reup=reup, ids=ids)
    data_mapping = get_mapping(reup=remap)

    for source, tables in sorted(data.items()):  # should probably sort?
        all_values = []
        rows = []
        with open('/tmp/%s.csv' % source, 'wt') as f:
            print('building csv for', source)
            writer = csv.writer(f, lineterminator='\n')
            writer.writerow(csv_schema)
            for table, columns in sorted(tables.items()):
                if any([table.startswith(prefix) for prefix in exclude_table_prefixes]):
                    continue
                elif table in exclude_tables:
                    continue
                mapping = data_mapping.get(table, None)
                ex_cols = exclude_columns.get(table, None)
                for column, values in sorted(columns.items()):
                    if ex_cols and column in ex_cols:  # yay! I hate this construction!
                        continue
                    if not mapping or column != mapping['name_col']:  # FIXME ineff?
                        map_ = None
                    else:
                        map_ = mapping['map']

                    for value in sorted([v for v in values if v is not None]):
                        #source, table, column, value  # all loop vars
                        if map_:
                            eid = map_.get(value, "WHY IS THIS MISSING?")
                            locals_ = locals()  # DIRTY EVIL EVIL
                            row = [locals_.get(col, None) for col in csv_schema]
                            rows.append(row)
                            continue
                        else:
                            eid = None  # overwrite so it doesnt hang in locals

                        if type(value) == str:
                            iv_cand_id_cat_prov = expand_map_value(column, value, split=False, skip={'search'}, continue_={'labels'})
                        else:
                            iv_cand_id_cat_prov = ((None, value, None, None, prov_order[-1]),)  # int

                        for input_value, candidate, identifier, category, prov in iv_cand_id_cat_prov:
                            #candidate, identifier,  # loop vars
                            #relation, prov, eid, ms, notes
                            locals_ = locals()  # DIRTY EVIL EVIL
                            row = [locals_.get(col, None) for col in csv_schema]
                            rows.append(row)

                        if iv_cand_id_cat_prov:
                            del(locals_)
                            del(input_value)
                            del(candidate)
                            del(identifier)
                            del(category)
                            del(prov)

                        all_values.append(value)  # simplify refine XXX deprecated

            #refined = refine(all_values)  # don't need to refine directly anymore
            #for row, ref in zip(rows, refined):
                #row[5] = ref
            for row in rows:
                writer.writerow(row)
            #return

def second_pass(file):  # TODO if singletons have already been mapped then try to do an autocorrect
    with open(file, 'rt') as f:
        rows = [l for l in csv.reader(f) if l]

    h = {c:i for i, c in enumerate(rows[0])}  # in leiu of namedtuple...
    
    existing = {}
    print(existing)
    for row in rows[1:]:
        #if not row:
            #continue  # WAT

        if row[h['relation']]:
            if row[h['candidates_2']]:
                existing[row[h['value']]] = row[h['candidates_2']], row[h['relation']]
            else:  # cands 1
                existing[row[h['value']]] = row[h['candidates_1']], row[h['relation']]  # shouldn't be an issue...
                
        if len(row[h['notes']]) <= 1:
            try:
                #if row[h['notes']]:
                assert_count = int(row[h['notes']])
                #elif not row[7]:  # stop trying to be clever
                    #assert_count = 0
            except ValueError:
                if row[h['notes']].startswith('exp'):
                    pass
                elif row[h['notes']]:
                    print('note was not an int!', row[h['notes']])
                continue
            count = 1
            notes_suffix = ''
            for sep in separators:
                if sep in row[h['candidates_1']]:
                    if sep == 'and' or sep == 'or':
                        count += row[h['candidates_1']].count(sep + ' ')  # sometimes in word
                    else:
                        count += row[h['candidates_1']].count(sep)
                    notes_suffix += ' ' + sep
            for cartsep in cart_seps:
                if cartsep in row[h['candidates_1']]:
                    count -= row[h['candidates_1']].count(cartsep)  # double counting

            if notes_suffix:
                #if assert_count and assert_count != count:
                if assert_count != count:
                    print('oops miscount detected', assert_count, count, '  ', row[h['candidates_1']])
                    count = assert_count
                row[h['notes']] = str(count) + notes_suffix

    print(existing)
    new_rows = [rows[0]]
    for row in rows[1:]:
        #if not row:
            #continue  # WUT
        count, seps = parse_notes(row[h['notes']])
        if seps:
            vals = [row[h['candidates_1']]]
            #while seps:
                #first, seps = seps.pop(0)
            for sep in seps:
                new_vals = []
                for val in vals:
                    new_val = [v.strip() for v in val.split(sep)]
                    new_vals.extend(new_val)
                vals = [v for v in new_vals if v]
            if len(vals) == count:
                for val in vals:
                    val = val.replace('(','').replace(')','')
                    val2, val3 = existing.get(val, (val, row[h['relation']]))  # see if we already curated the singles
                    if val != val2:
                        print('success!', val, val2)
                        #val3 = row[h['relation']]  # TODO existing!
                    new_row = row[:h['curator_candidates']] + [val, val2, val3] + row[h['eid']:]  # pass by ref issues
                    new_row[h['notes']] = 'was ' + new_row[h['notes']]  # prevent double expands
                    new_rows.append(new_row)
            else:
                row[h['notes']] = 'was ' + row[h['notes']]
                for _ in range(count):
                    new_rows.append(row)
        else:
            row[h['notes']] = 'was ' + row[h['notes']]
            for _ in range(count):
                new_rows.append(row)

    output_filename = file + '.2.csv'
    with open(output_filename, 'wt') as f:
       writer = csv.writer(f, lineterminator='\n')
       writer.writerows(new_rows)

    return output_filename

def clean_whitespace(file):
    with open(file, 'rt') as f:
        rows = [l for l in csv.reader(f)]
    rows = [[cr.strip() if i > 4 else cr for i, cr in enumerate(r)] for r in rows]  # clean the whitespace
    with open(file + '.cleaned', 'wt') as f:
       writer = csv.writer(f, lineterminator='\n')
       writer.writerows(rows)

def third_pass(file):
    with open(file, 'rt') as f:
        rows = [l for l in csv.reader(f)]

    h = {c:i for i, c in enumerate(rows[0])}

    for row in rows[1:]:
        row[h['relation']] = row[h['relation']].strip()  # cleanup the mess
        if row[h['candidates_2']]:
            out = v.findByTerm(row[h['candidates_2']].strip(), searchSynonyms=False)  # TODO thread this!
            if out:
                to_join = [r['curie'] for r in out]
                if None in to_join:
                    print('WTF ROW:', row, [(r['curie'], r['labels']) for r in out])
                    to_join = [c for c in to_join if c]
                uberon = [c for c in to_join if c and c.startswith('UBERON:')]
                row[h['candidates_2']] = uberon[0] if len(uberon) == 1 else '|'.join(to_join)
            else:
                print('CURATOR CAND NOT FOUND WTF:', row[h['candidates_2']])
        else:
            out = v.findByTerm(row[h['candidates_1']].strip(), searchSynonyms=False)  # TODO thread this!
            if out:
                to_join = [r['curie'] for r in out]
                if None in to_join:
                    print('WTF ROW:', row, [(r['curie'], r['labels']) for r in out])
                    to_join = [c for c in to_join if c]
                uberon = [c for c in to_join if c and c.startswith('UBERON:')]
                row[h['candidates_1']] = uberon[0] if len(uberon) == 1 else '|'.join(to_join)
            else:
                pass
                #print('CAND NOT FOUND WTF:', row[3], row[h['candidates_1']])

    output_filename = file + '.3.csv'
    with open(output_filename, 'wt') as f:
       writer = csv.writer(f, lineterminator='\n')
       writer.writerows(rows)

    return output_filename

def select_id(ids):
    prefix_preferred = 'NCBITaxon', 'UBERON', 'DOID', 'PR'
    id_preferred = {  # FIXME
                    'testid',
                    'NIFCELL:nifext_17',
    }
    ranker = {id_:0 for id_ in ids}
    for id_ in ids:
        prefix, suffix = id_.split(':')
        if prefix in prefix_preferred:
            ranker[id_] += 1
        if id_ in id_preferred:
            ranker[id_] += 1

    ranked = sorted(ranker.items(), key=lambda a: -a[1])
    if 0 and not any(ranker.values()):
        print('-----')
        for id_ in ids:
            out = v.findById(id_)
            print(id_, out['labels'][0], out['definitions'])
    #print(ranked)
    return ranked[0][0]

def reduce_cand_row(row, ci):
    candidates = row[ci['candidates_1']], row[ci['candidates_2']]
    identifiers = []
    if not row[ci['relation']]:
        #if [_ for _ in candidates if ':' in _]:
            #print('To curate:', row)
        return None
    elif row[ci['relation']] not in valid_relations:
        raise ValueError('invalid relation: %s' % row[ci['relation']])


    for c in candidates:
        c = c.strip()  # >_<
        if ':' in c:
            if ' ' in c:
                continue  # FIXME nasty
            elif '|' in c:
                selected = select_id(c.split('|'))
                identifiers.append(selected)
            else:
                identifiers.append(c)
                
    identifier = None
    status = row[ci['status']]
    if len(identifiers) > 1:
        #print('MORE THAN ONE UNIQUE CANDIDATE')
        raise BaseException('more than one candidate %s' % identifiers)
    elif identifiers:
        identifier = identifiers[0]
        status = 'curated'
    else:
        #print('no identifier for', row)
        return None  # OK for now, don't want to insert unmapped just yet?

    rest_insert = {
        'source':row[ci['source']],
        'table':row[ci['table']],
        'column':row[ci['column_name']],
        'value':row[ci['value']],
        'identifier':identifier,
        'external_id':row[ci['eid']],
        'relation':row[ci['relation']].strip(),
        'curation_status':status,
    } 
    return rest_insert

def upload_mappings(file, keyfile):
    """ upload to database """
    with open(file, 'rt') as f:
        reader = csv.reader(f)
        rows = [l for l in reader if l]

    col_names = rows[0]
    rows = rows[1:]
    
    #print(col_names)
    #embed()
    ci = {k:i for i, k in enumerate(col_names)}
    to_insert = []
    for i, r in enumerate(rows):
        try:
            to_insert.append(reduce_cand_row(r, ci))
        except ValueError as e:
            print(e, 'on row', i+2)

    need_to_finish = [r for r, test in zip(rows, to_insert) if not test]
    to_insert = [r for r in to_insert if r]

    #csv_cols = ('source', 'table', 'column_name','value', MANY, 'external_id', 'relation','status')
    #db_cols = ('source', 'table_name', 'col', 'value', 'identifier', 'external_id', 'relation', 'curation_status')

    
    upload_url_prefix = 'https://stage.scicrunch.org'
    base_url = upload_url_prefix + '/api/1/entitymapping/add/{source}/{table}/{column}/{value}'
    #identifier, external_id, relation, match_substring, status, curation_status

    #DB_URI = 'mysql+mysqlconnector://{user}:{password}@{host}:{port}/{db}'
    #config = mysql_conn_helper('mysql5-stage.crbs.ucsd.edu', 'nif_eelg', 'nif_eelg_secure')
    #engine = create_engine(DB_URI.format(**config))
    #config = None  # all weakrefs should be gone by now?
    #del(config)  # i wonder whether this actually cleans it up when using **config
    #conn = engine
    #sql = 'INSERT INTO entity_mapping {columns} VALUES {values};'.format(columns=db_cols, values=to_insert)
    #print(sql)
    #conn.execut()

    
    #insp = inspect(engine)
    #names = [c['name'] for c in insp.get_columns('entity_mapping')]
    #resource_columns = [c['name'] for c in insp.get_columns('resource_columns')]
    #resource_data = [c['name'] for c in insp.get_columns('resource_data')]
    #resource_fields = [c['name'] for c in insp.get_columns('resource_fields')]
    #resources = [c['name'] for c in insp.get_columns('resources')]

    if keyfile != None:
        if path.exists(keyfile):
            with open(keyfile, 'rt') as f:
                apikey = f.read().strip()

            post_results = []
            for dict_ in to_insert:
                dict_['key'] = apikey
                result = requests.post(base_url.format(dict_), data=dict_)  # this should be ok?
                post_results.append(result)
        else:
            raise IOError('Keyfile not found: %s' % keyfile)
    else:
        print('data that would be inserted')
        repr_insert(to_insert)

    #embed()
    
def repr_insert(to_insert):
    print('{column: <20}{value: <39} {identifier: <32}{relation: <12}{curation_status}{external_id}'.format(**{k:k for k in to_insert[0]}))
    for dict_ in to_insert:
        md = {k:v for k, v in dict_.items()}
        if len(md['value']) > 39:
            md['value'] = md['value'][:36] + '...'
        print('{column: <20}{value: <39} {identifier: <32}{relation: <12}{curation_status}{external_id}'.format(**md))
    #for a,b,c,d,e,f,g,h in sorted(to_insert):
        #print(a, b, c, '{: <40}'.format(d), '{: <20}'.format(e), f, g, h)
        #print('{: <20}{: <40}{: <32}{}{: <12}{}'.format(c, d, e, f, g, h))

def main():
    args = docopt(__doc__, version='entity_map .0002')
    print(args)
    files = args['<files>']

    if args['make']:
        make_csvs(args['<identifiers>'], args['--reup'], args['--remap'])
        if no_curies:
            print('bad curies')
            print(no_curies)

    if args['second']:
        new_files = []
        for file in files:
            output = second_pass(file)
            new_files.append(output)

    if args['third']:  # we do the mapping now
        if args['second']:
            files = new_files
        new_files = []
        for file in files:
            output = third_pass(file)
            new_files.append(output)

    if args['upload']:
        if args['third']:
            files = new_files
        for file in files:
            upload_mappings(file, args['--apikeyfile'])

    if args['clean']:
        for file in files:
            clean_whitespace(file)

if __name__ == '__main__':
    main()
