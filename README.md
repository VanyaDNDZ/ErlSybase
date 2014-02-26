ErlSybase
=========

Sybase nif driver for erlang


Usage
=========

run erlang 

start driver

> sybdrv:start([]).

execute query with params 
> sybdrv:execQueryWithArgs(p48testab,"select * FROM temp_gh WHERE val=?",["980"]).

execute query w/o params 
> sybdrv:execQuery(p48testab,"select * FROM temp_gh WHERE val='980'").

call procedure(select)

> sybdrv:execQuery(p48testab,"exec mydb..get_my_data").
This method return first result set.

call procedure(update)

> sybdrv:execCallProc(p48testab,"exec mydb..get_my_data",[]).
This method return status of sp.


