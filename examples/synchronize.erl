#!/usr/bin/env escript
%%! -pa ../ebin -pa ebin -sname synchronize

%-module (bulk_jobs).
%-export ([main/1]).

wait_job (JobID) ->
  {ok, {job_id, JobID}, {exit, Exit}, {exit_status, ExitStatus}, {usage, Usage}} = drmaa:wait (JobID, no_wait),
  io:format ("~p: ~n", [JobID]),
  io:format ("  Exit: ~p~n  Exit status: ~p~n", [Exit, ExitStatus]),
  io:format ("  Usage: ~p~n~n", [Usage]).

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

  {ok, Jobs} = drmaa:run_jobs (2),
  io:format ("Jobs has been submitted with ids: ~p~n", [Jobs]),

  {ok, JobsCount} = drmaa:synchronize (Jobs, 30000),
  io:format ("Synchronized well: ~p~n", [JobsCount]),

  lists:foreach (fun (A) -> wait_job (A) end, Jobs),

  {ok} = drmaa:delete_job_template (),

  ok.
