#!/usr/bin/env python3
"""\
Usage:
    entity_map [ --help ]
    entity_map second third upload (<files>...)
    entity_map third upload (<files>...)
    entity_map first
    entity_map second (<files>...)
    entity_map third (<files>...)
    entity_map upload [--apikeyfile=APIKEYFILE] (<files>...)
    entity_map clean (<files>...)

Options:
    --apikeyfile=APIKEYFILE     actually do the upload using the specified api key
"""
import json
import csv
from os import path
import requests
from docopt import docopt
from IPython import embed
from heatmaps.services import database_service
from pyontutils.scigraph_client import Refine, Vocabulary
#from pyontutils.scr_sync import mysql_conn_helper, create_engine, inspect
from collections import namedtuple

v = Vocabulary()

class discodv(database_service):
    dbname = 'disco_crawler'
    user = 'discouser'
    host = 'nif-db.crbs.ucsd.edu'
    port = 5432
    DEBUG = True

valid_relations = ('exact','part of','subClassOf')
ids = (
    #'*MAPPING*',  # utility for name -> id in ERDs  # XXX do not use, breaks the first for loops
    'nif_0000_00006',  # morpho
    'nif_0000_00508',  # allen celltypes
    'nlx_151885',  # nele
    'nif_0000_37639',  # cil

    #'nlx_154697',  # integrated connectivity FIXME must run on dv not public
)
skip_prefixes = ('pr_', 'temp_', 'ti_') # pr is production, temp is temporary, ti is still being crawled (ingested)
no_map_columns = {  # TODO cull these using the column mappings
                  'l2_nlx_151885_data_ephys':(
                      'id',
                      'e_uid',
                      'nelx_id',
                  ),
                  'l2_nlx_151885_data_neuron':(
                      'id',
                      'e_uid',
                      'nelx_id',
                  ),
                  'l2_nlx_151885_data_summary':(
                      'e_uid',
                      'id',
                      'num_nedms',
                      'value_sd',
                      'num_articles',
                      'n_id',
                      'e_id',
                      'value_mean',
                      'nelx_id',
                  ),
                  'l2_nif_0000_00006_data_nif_neuron':(
                      'volume',
                      'url_reference',
                      'surface',
                      'soma_surface',
                      'png_url',
                      'min_age',
                      'max_age',
                      'min_weight',
                      'max_weight',  # TODO UNITS! probably @ column level
                      'neuron_id',
                      'e_uid',
                      'neuron_name',  # these are the experimenter supplied 'names' ie exp local identifiers
                      'note',  # TODO these may contain some useful methods data...
                  ),
                  'l2_nif_0000_00006_data_neuron_article':(
                      'e_uid',
                      'neuron_id',
                      'ocdate',
                      'article_id',
                      'pmid',
                  ),
                  'l2_nif_0000_00006_data_allpublications':(
                      'e_uid',
                      'doi',
                      'paper_id',
                      'pmid',
                      'year',
                      'article_id',
                      'ocdate',
                      'paper_title',  # TODO tagging withing using 'mentions' ????
                      'related_page',
                      'first_author',  # TODO ORCID/neurotree mapping???
                      'last_author',  # TODO ORCID/neurotree
                      'journal',  # TODO who tracks these fellows?
                      'no_of_reconstructions',
                  ),
                  'l2_nif_0000_37639_image_imageannotation':(
                      'e_uid',
                      'id',
                      'entry_value',
                      'entry_ns',
                  ),
                  'l2_nif_0000_37639_image_imagedetail':(
                      'e_uid',
                      'id',
                      'xml',
                      'attribution',
                      'file_size',
                      'group_name',
                      'relative_file_path',
                      'imagedescription',  # TODO tagging inside?
                      'name',  # this is the file name
                  ),
                  'l2_nif_0000_37639_onto_label':(
                      'onto_id',  # FIXME map this

                  ),
                  'l2_nif_0000_00508_specimen_donor':(
                      #'age_id',  # we DO need to map this ;_; BUT HOW
                      'e_uid',
                      'external_donor_name',  # do need to map :/
                      'id',
                      'name',  # XXX there are two additional annotations that I cannot figure out in these: IVSCC and GSL
                      'specimen_id',
                      'tags',  # compound again
                  ),
                  'l2_nif_0000_00508_specimen_ephys_feature':(
                      'adaptation',
                      'avg_isi',
                      'blowout_voltage',
                      'e_uid',
                      'electrode_0_pa',
                      'f_i_curve_slope',
                      'fast_trough_t_long_square',
                      'fast_trough_t_ramp',
                      'fast_trough_t_short_square',
                      'fast_trough_v_long_square',
                      'fast_trough_v_ramp',
                      'fast_trough_v_short_square',
                      'id',
                      'initial_access_resistance',
                      'input_resistance_mohm',
                      'latency',
                      'peak_t_long_square',
                      'peak_t_ramp',
                      'peak_t_short_square',
                      'peak_v_long_square',
                      'peak_v_ramp',
                      'peak_v_short_square',
                      'rheobase_sweep_id',
                      'ri',
                      'sag',
                      'seal_gohm',
                      'slow_trough_t_long_square',
                      'slow_trough_t_ramp',
                      'slow_trough_t_short_square',
                      'slow_trough_v_long_square',
                      'slow_trough_v_ramp',
                      'slow_trough_v_short_square',
                      'specimen_id',
                      'tau',
                      'threshold_i_long_square',
                      'threshold_i_ramp',
                      'threshold_i_short_square',
                      'threshold_t_long_square',
                      'threshold_t_ramp',
                      'threshold_t_short_square',
                      'threshold_v_long_square',
                      'threshold_v_ramp',
                      'threshold_v_short_square',
                      'thumbnail_sweep_id',
                      'trough_t_long_square',
                      'trough_t_ramp',
                      'trough_t_short_square',
                      'trough_v_long_square',
                      'trough_v_ramp',
                      'trough_v_short_square',
                      'upstroke_downstroke_ratio_long_square',
                      'upstroke_downstroke_ratio_ramp',
                      'upstroke_downstroke_ratio_short_square',
                      'vm_for_sag',
                      'vrest',
                  ),  # I need to do the column mapping here!  (freeking er)
                  'l2_nif_0000_00508_specimen_neuron_reconstruction':(
                      'average_bifurcation_angle_local',
                      'average_bifurcation_angle_remote',
                      'average_contraction',
                      'average_diameter',
                      'average_fragmentation',
                      'average_parent_daughter_ratio',
                      'e_uid',
                      'hausdorff_dimension',
                      'id',
                      'max_branch_order',
                      'max_euclidean_distance',
                      'max_path_distance',
                      'number_bifurcations',
                      'number_branches',
                      'number_nodes',
                      'number_stems',
                      'number_tips',
                      'overall_depth',
                      'overall_height',
                      'overall_width',
                      'scale_factor_x',
                      'scale_factor_y',
                      'scale_factor_z',
                      'soma_surface',
                      'specimen_id',
                      #'tags',
                      'total_length',
                      'total_surface',
                      'total_volume',
                  ),
                  'l2_nif_0000_00508_specimen_specimen_tag':(
                      'ar_association_key_name',
                      'e_uid',
                      'id',
                      #'name',
                      'specimen_id',
                  ),
                  'l2_nif_0000_00508_specimen_structure':(
                      #'acronym',
                      'atlas_id',
                      'color_hex_triplet',
                      'depth',
                      'e_uid',
                      #'failed',  # need to know what this actually means
                      'failed_facet',
                      'graph_id',
                      'graph_order',
                      'hemisphere_id',
                      'id',
                      #'name',
                      'ontology_id',
                      'parent_structure_id',
                      #'safe_name',
                      'specimen_id',
                      'sphinx_id',
                      'structure_id_path',
                      'structure_name_facet',
                      'weight',
                  ),
                  'l2_nif_0000_00508_specimen_summary':(
                      'donor_id',
                      'e_uid',
                      'ephys_result_id',
                      'failed_facet',
                      #'hemisphere',  # this needs to be mapped
                      'id',
                      'is_cell_specimen',
                      'is_ish',
                      'name',  # same issue as w/ donor, IVSCC and GSL are a mystery
                      'parent_id',
                      'parent_x_coord',
                      'parent_y_coord',
                      'parent_z_coord',
                      'specimen_id_path',
                      'sphinx_id',
                      'structure_id',
                      'weight',
                  ),
                  'l2_nif_0000_00508_specimen_transgenic_line':(
                      'ar_association_key_name',
                      'description',
                      'donor_id',
                      'e_uid',
                      'id',
                      #'name',
                      #'originating_lab',
                      'specimen_id',
                      'stock_number',
                      'sub_image_annotation_id',
                      #'transgenic_line_source_name',
                      #'transgenic_line_type_code',
                      #'transgenic_line_type_name',
                      'url_prefix',
                      'url_suffix',
                  ),

                  #'relative_file_path',  # nasty one in??
                  #'id',
                  #'id1',
                  #'v_uid',
                  #'v_uuid',
                  #'pmid',
                  #'doi',
                  #'url',
                  #'url_reference',
                 }
table_skip = (
    'l2_nif_0000_00508_data_gene',
    'l2_nif_0000_00508_data_gene_expression',
)

external_id_map = {
    'l2_nlx_151885_data_summary':('n_name', 'nelx_id'),
    'l2_nlx_151885_data_neuron':('name', 'nelx_id'),
    'l2_nif_0000_37639_onto_label':('name', 'onto_id'),
}

ont_pref = (
    'http://purl.obolibrary.org/obo/',
    'http://ontology.neuinfo.org/NIF/',
)

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
        def superinner(reup=False):
            if path.exists(filepath) and not reup:
                print('deserializing from', filepath)
                with open(filepath, 'r' + mode) as f:
                    return deserialize(f)
            else:
                output = function()
                with open(filepath, 'w' + mode) as f:
                    serialize(output, f)
                return output

        return superinner

    return inner

@memoize('/home/tom/files/entity_mapping/ents.json')
def get_data():
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
                    #if column in no_map_columns:  # move to csv gen?
                        #continue
                    SQL = "SELECT DISTINCT(%s) FROM %s" % (column, table_name)  # XXX DANGER ZONE
                    values = dv.cursor_exec(SQL)
                    #print(values)
                    columns[column] = [v if len(v) > 1 else v[0] for v in values]

        print([v.keys() for v in data.values()])

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

            print(mapping)
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

separators = (',', ';', '&', 'and', 'or')
cart_seps = []
for r in [[s1 + ' ' + s2 for s2 in separators] for s1 in separators]:
    cart_seps.extend(r)

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
                    new_row = row[:h['curator_candidates']] + [val, val2, val3] + row[h['external_id']:]  # pass by ref issues
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
        'external_id':row[ci['external_id']],
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
    args = docopt(__doc__, version='entity_map .0001')
    print(args)
    files = args['<files>']

    if args['first']:
        data = get_data()#reup=True)
        data_mapping = get_mapping()#reup=True)
        #embed()
        #return

        for source, tables in sorted(data.items()):  # should probably sort?
            all_values = []
            rows = []
            with open('/tmp/%s.csv' % source, 'wt') as f:
                print('building csv for', source)
                writer = csv.writer(f, lineterminator='\n')
                writer.writerow(['source', 'table', 'column_name',
                                 'value', 'candidates_1',
                                 'curator_candidates', 'candidates_2',
                                 'relation', 'external_id',
                                 'match_substring', 'status', 'notes'])
                for table, columns in sorted(tables.items()):
                    if any([table.startswith(prefix) for prefix in skip_prefixes]):
                        continue
                    elif table in table_skip:
                        continue
                    mapping = data_mapping.get(table, None)
                    nmv = no_map_columns.get(table, None)
                    for column, values in sorted(columns.items()):
                        if nmv and column in nmv:  # yay! I hate this construction!
                            continue
                        if not mapping or column != mapping['name_col']:  # FIXME ineff?
                            map_ = None
                        else:
                            map_ = mapping['map']

                        for value in sorted([v for v in values if v is not None]):
                            if map_:
                                external_id = map_.get(value, "WHY IS THIS MISSING?")
                            else:
                                external_id = ''

                            refined_candidates = None
                            curator_cands = ''
                            relation = ''
                            match_substring = ''
                            status = ''
                            notes = ''
                            row = [source, table, column,
                                   value, value,
                                   curator_cands, curator_cands,
                                   relation, external_id,
                                   match_substring, status, notes]
                            rows.append(row)
                            all_values.append(value)  # simplify refine

                #refined = refine(all_values)  # don't need to refine directly anymore
                #for row, ref in zip(rows, refined):
                    #row[5] = ref
                for row in rows:
                    writer.writerow(row)
                #return
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
