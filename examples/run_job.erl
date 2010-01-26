#!/usr/bin/env escript
%%! -pa ../ebin -pa ebin -sname run_job

main (_) ->
  drmaa:start_link (),
  {ok} = drmaa:allocate_job_template (),
  {ok} = drmaa:remote_command ("sleeper.sh"),
  {ok} = drmaa:args (["42", "Saymon says: "]),
  {ok} = drmaa:join_files (true),

  {ok, JobID} = drmaa:run_job (),
  io:format ("Your job has been submitted with id: ~p~n", [JobID]),

  {ok} = drmaa:delete_job_template (),

  ok.
