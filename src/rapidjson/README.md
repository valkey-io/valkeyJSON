# RapidJSON Source Code
* The original RapidJSON Source Code is cloned at build time using CMAKELISTS
* Last commit on the master branch: 0d4517f15a8d7167ba9ae67f3f22a559ca841e3b, 2021-10-31 11:07:57

# Modifications
We made a few changes to the RapidJSON source code. Before the changes are pushed to the open source,
we have to include a private copy of the file. Modified RapidJSON code is under src/rapidjson.

## document.h`
We need to modify RapidJSON's document.h to support JSON depth limit. 

### reader.h
Modified reader.h to only generate integers in int64 range.

