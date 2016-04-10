#!/usr/bin/env python3
import csv
from pyontutils.scigraph_client import Graph

g = Graph('http://localhost:9000/scigraph')

with open('/tmp/nlx_154697_8.csv','rt') as f:
    rows = [r for r in csv.reader(f)]
    header = rows[0]
    index = {name:index for index, name in enumerate(header)}
    rows = rows[1:]

dbx = 'http://www.geneontology.org/formats/oboInOwl#hasDbXref'
with open('/tmp/nlx_154697_8_fma.csv','wt') as f:
    writer = csv.writer(f)
    writer.writerow(header[:index['identifier']+1] + ['fma_id'] + header[index['identifier']+1:])
    for row in rows:
        fma_id = ''
        id_ = row[index['identifier']]
        if id_.startswith('UBERON'):
            meta = g.getNode(id_)['nodes'][0]['meta']
            if dbx in meta:
                xrefs = meta[dbx]
                for ref in xrefs:
                    if ref.startswith('FMA:'):
                        fma_id += ref

        writer.writerow(row[:index['identifier']+1] + [fma_id] + row[index['identifier']+1:])


