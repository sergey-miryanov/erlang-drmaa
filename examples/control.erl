#!/usr/bin/env escript
%%! -pa ../ebin -pa ebin -sname control

main (_) ->
  drmaa:start_link (),
  {ok} = drmaa:allocate_job_template (),

  {ok} = drmaa:working_dir ("/home/zerg/works/drmaa/examples"),
  {ok} = drmaa:join_files (true),
  {ok} = drmaa:remote_command ("/home/zerg/works/drmaa/examples/sleeper.sh"),
  {ok} = drmaa:args (["42", "\"Saymon says: \"", "30s"]),
  {ok} = drmaa:output_path ("/home/zerg/works/drmaa/"),

  {ok, JobID} = drmaa:run_job (),
  io:format ("Job has been submitted with id: ~p~n", [JobID]),

  {ok} = drmaa:control (JobID, suspend), io:format ("suspended~n"),
  {ok} = drmaa:control (JobID, resume), io:format ("resumed~n"),
  {ok} = drmaa:control (JobID, hold), io:format ("hold~n"),
  {ok} = drmaa:control (JobID, release), io:format ("released~n"),
  {ok} = drmaa:control (JobID, terminate), io:format ("terminated~n"),

  WaitResult = drmaa:wait (JobID, 30000),
  io:format ("~p~n", [WaitResult]),

  {ok} = drmaa:delete_job_template (),

  ok.
