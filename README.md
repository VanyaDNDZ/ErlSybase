ErlSybase
=========

Sybase nif driver for erlang
rewritten from [erlang-db-driver port driver](https://github.com/denglf/erlang-db-driver) 

Usage
=========

0.Run erlang <br>
<code>erl ebin/ deps/*/ebin</code>

```erl
%%Start driver
1> sybdrv:start([])

%%Execute query with params 
2> sybdrv:execQueryWithArgs(testdb,"select * FROM temp_gh WHERE val=?",["980"]).


%%Execute query with param date
3> sybdrv:execQueryWithArgs(testdb,"select top 100 *  from mySuperTAble where createdate=?",[{datetime,{{2008,7,3},{8,0,0,0}}}]).

%%Execute  query with non-ASCII params
4> sybdrv:execQueryWithArgs(testdb,"select * from Oranization where Name=?",[binary:bin_to_list(unicode:characters_to_binary("Благотворительная организация"))]).

%%Execute  query w/o params
5> sybdrv:execQuery(testdb,"select * FROM temp_gh WHERE val='980'").

%%Call procedure(select)
%%This method return first result set.
6> sybdrv:execQuery(testdb,"exec mydb..get_my_data").

%% Call procedure(update)
%%  This method return status of sp.
7> sybdrv:execCallProc(testdb,"exec mydb..get_my_data",[]).

```
Alternatives
============
* [jamdb_sybase](https://github.com/erlangbureau/jamdb_sybase)
* [Erlang ODBC](http://www.erlang.org/doc/man/odbc.html)
* [erlang-db-driver](https://github.com/denglf/erlang-db-driver)
* [erldb-driver](https://github.com/RYTong/erldb-driver)
