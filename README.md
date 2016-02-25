# entity_mapping
repository for SciCrunch entity mapping

## Dependencies
docopt
IPython
requests
[tgbugs/pyontutils](https://github.com/tgbugs/pyontutils)
[tgbugs/heatmaps](https://github.com/tgbugs/heatmaps)

## TODO
000. seriously consider what to do about the need to curate against rows not just strings
00. move column mapping to its own file and long run data service
0. switch to a successive refinement model, no more open refine
1. potential match phases: labels, synonyms, abbreviations, accronyms, search, curator supplied
2. web service that takes the 'curated' csv, sends the curated entries to a head curator for review and will then send the changes back to the original curator so they can see things for future reference, on upload of the terms by the head curator the match should
3. need to show category or super class labels for truly opaque identifiers
4. attempt to autosplit on known dividers such as , ; and or etc.
5. curation process will then involve adding the relation to matches or duplicating rows that are needed
6. after the autosplit expand all the candidates based on phase and add a rowfor each potential match
7. match column type (mapping) to category or super class so we can run a sanity check on potential matches
8. new spreadsheet format: source, table, column, value, candidate, identifier, relation, prov, external_id, match_substring, notes
9. prov values: synonym, abbrev, accronym, search, curator. These correspond to the phases for successive refinement listed above
10. relation is the key field for curation, values: exact, part of, subClassOf, located in, NOMAP. Unmapped/unneeded rows can be left blank, need to figure out whether to auto delete or not.

