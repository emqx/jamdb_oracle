-module(jamdb_oracle_buffer).

-export([get/1, set/2, del/1]).

get(Key) ->
    erlang:get(Key).

set(Key, Values) ->
    erlang:put(Key, Values),
    ok.

del(Key) ->
    erlang:erase(Key),
    ok.
