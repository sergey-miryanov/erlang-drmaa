#!/usr/bin/env escript
%%! -pa ../ebin -pa ebin -sname job_ids

main (_) ->
  drmaa:start_link (),

  JobIdsSessionAll = drmaa:job_ids (all),
  JobIdsSessionAny = drmaa:job_ids (any),

  io:format ("ALL: ~p~n", [JobIdsSessionAll]),
  io:format ("ANY: ~p~n", [JobIdsSessionAny]),

  ok.
