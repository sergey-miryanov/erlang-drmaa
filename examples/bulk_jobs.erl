#!/usr/bin/env escript
%%! -pa ../ebin -pa ebin -sname bulk_jobs

%-module (bulk_jobs).
%-export ([main/1]).

main (_) ->
  drmaa:start_link (),
  {ok} = drmaa:allocate_job_template (),

  {ok} = drmaa:working_dir ("/home/zerg/works/drmaa/examples"),
  {ok} = drmaa:join_files (true),
  {ok} = drmaa:remote_command ("/home/zerg/works/drmaa/examples/sleeper.sh"),
  {ok} = drmaa:args (["42", "Saymon says: ", "3s"]),
  OutputPath ="/home/zerg/works/drmaa/bulk_jobs.out." ++ drmaa:placeholder (incr),
  io:format ("~p~n", [OutputPath]),
  {ok} = drmaa:output_path (OutputPath),

  {ok, Jobs} = drmaa:run_jobs (10),
  io:format ("Jobs has been submitted with ids: ~p~n", [Jobs]),

  {ok} = drmaa:delete_job_template (),

  ok.
