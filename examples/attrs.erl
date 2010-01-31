#!/usr/bin/env escript
%%! -pa ../ebin -pa ebin -sname attrs

%-module (attrs).
%-export ([main/1]).

main (_) ->
  drmaa:start_link (),
  {ok} = drmaa:allocate_job_template (),

  {ok}                    = drmaa:block_email (false),
  {ok}                    = drmaa:block_email (true),
  {error, not_supported}  = drmaa:deadline_time (""),
  {error, not_supported}  = drmaa:hlimit_duration (""),
  {error, not_supported}  = drmaa:slimit_duration (""),
  {ok}                    = drmaa:error_path ("/home/zerg/works/drmaa/examples"),
  {error, not_supported}  = drmaa:input_path ("/home/zerg/works/drmaa/examples/sleeper.sh"),
  {error, not_supported}  = drmaa:job_category (""),
  {ok}                    = drmaa:job_name ("attr"),
  {ok}                    = drmaa:job_state (active),
  {error, not_supported}  = drmaa:native_spec (""),
  {ok}                    = drmaa:output_path ("/home/zerg/works/drmaa/examples"),
  {error, not_supported}  = drmaa:start_time (""),
  {ok}                    = drmaa:transfer_files ([input, error, output]),
  {ok}                    = drmaa:emails (["zerger@gmail.com"]),
  {error, not_supported}  = drmaa:hlimit (""),
  {error, not_supported}  = drmaa:slimit (""),
  {ok}                    = drmaa:working_dir ("/home/zerg/works/drmaa/examples"),
  {ok}                    = drmaa:join_files (false),
  {ok}                    = drmaa:remote_command ("sleeper.sh"),
  {ok}                    = drmaa:args (["42", "Saymon says: "]),
  {ok}                    = drmaa:env ([]),

  {ok, JobID} = drmaa:run_job (),
  io:format ("Job has been submitted with id: ~p~n", [JobID]),

  {ok, {exit, Exit}, {exit_status, ExitStatus}, {usage, Usage}} = drmaa:wait (JobID, infinity),
  io:format ("Exit: ~p~nExit status: ~p~n", [Exit, ExitStatus]),
  io:format ("Usage: ~p~n", [Usage]),

  {ok} = drmaa:delete_job_template (),

  ok.
