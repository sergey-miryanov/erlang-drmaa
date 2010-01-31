#!/usr/bin/env escript
%%! -pa ../ebin -pa ebin -sname job_status

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

  {ok} = drmaa:control (JobID, suspend), 
  {ok, SuspendedState} = drmaa:job_status (JobID),
  io:format ("Job status: ~p~n", [SuspendedState]),

  {ok} = drmaa:control (JobID, resume), 
  {ok, ResumedState} = drmaa:job_status (JobID),
  io:format ("Job status: ~p~n", [ResumedState]),

  {ok} = drmaa:control (JobID, hold), 
  {ok, HoldState} = drmaa:job_status (JobID),
  io:format ("Job status: ~p~n", [HoldState]),

  {ok} = drmaa:control (JobID, release), 
  {ok, ReleasedState} = drmaa:job_status (JobID),
  io:format ("Job status: ~p~n", [ReleasedState]),

  {ok} = drmaa:control (JobID, terminate), 
  {ok, TerminatedState} = drmaa:job_status (JobID),
  io:format ("Job status: ~p~n", [TerminatedState]),

  WaitResult = drmaa:wait (JobID, 30000),
  io:format ("~p~n", [WaitResult]),

  {ok} = drmaa:delete_job_template (),

  ok.
