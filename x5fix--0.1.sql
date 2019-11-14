  
create or replace function add_entry_gp_persistent_filespace_node(fs oid, pdbid smallint, ppath text, mdbid smallint, mpath text)
returns bool
as '$libdir/x5fix.so','add_entry_gp_persistent_filespace_node'
language c immutable strict;