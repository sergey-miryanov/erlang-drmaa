#!/usr/bin/env escript
%%! -pa ../ebin -pa ebin -sname wait_job

main (_) ->
  drmaa:start_link (),
  {ok} = drmaa:allocate_job_template (),
  {ok, Cwd} = file:get_cwd (), 
  {ok} = drmaa:remote_command (filename:join ([Cwd, "examples", "sleeper.sh"])),
  {ok} = drmaa:args (["42", "Saymon says: ", "3s"]),
  {ok} = drmaa:join_files (true),

  {ok, JobID} = drmaa:run_job (),
  io:format ("Your job has been submitted with id: ~p~n", [JobID]),

  {ok, {job_id, JobID}, {exit, Exit}, {exit_status, ExitStatus}, {usage, Usage}} = drmaa:wait (JobID, infinity),
  io:format ("JobID: ~p~n", [JobID]),
  io:format ("Exit: ~p~nExit status: ~p~n", [Exit, ExitStatus]),
  io:format ("Usage: ~p~n", [Usage]),

  {ok} = drmaa:delete_job_template (),

  ok.
