#!/usr/bin/env escript
%%! -pa ../ebin -pa ebin -sname run_job

main (_) ->
  drmaa:start_link (),
  {ok} = drmaa:allocate_job_template (),
  {ok, Cwd} = file:get_cwd (), 
  {ok} = drmaa:remote_command (filename:join ([Cwd, "examples", "sleeper.sh"])),
  {ok} = drmaa:args (["42", "Saymon says: ", "3s"]),
  {ok} = drmaa:join_files (true),

  {ok, JobID} = drmaa:run_job (),
  io:format ("Your job has been submitted with id: ~p~n", [JobID]),

  {ok} = drmaa:delete_job_template (),

  ok.
