#!/usr/bin/env escript
%%! -pa ../ebin -pa ebin -sname sync_all

main (_) ->
  drmaa:start_link (),
  {ok} = drmaa:allocate_job_template (),

  {ok} = drmaa:working_dir ("/home/zerg/works/drmaa/examples"),
  {ok} = drmaa:join_files (true),
  {ok} = drmaa:remote_command ("/home/zerg/works/drmaa/examples/sleeper.sh"),
  {ok} = drmaa:args (["42", "Saymon says: "]),
  OutputPath ="/home/zerg/works/drmaa/bulk_jobs.out." ++ drmaa:placeholder (incr),
  io:format ("~p~n", [OutputPath]),
  {ok} = drmaa:output_path (OutputPath),

  {ok, Jobs} = drmaa:run_jobs (3),
  io:format ("Jobs has been submitted with ids: ~p~n", [Jobs]),

  {ok, JobsCount} = drmaa:synchronize ([all], 30000),
  io:format ("Synchronized well: ~p~n", [JobsCount]),

  X = drmaa:wait (any, no_wait), io:format ("~p~n", [X]),
  Y = drmaa:wait (any, infinity), io:format ("~p~n", [Y]),
  Z = drmaa:wait (any, 3000), io:format ("~p~n", [Z]),

  {ok} = drmaa:delete_job_template (),

  ok.
