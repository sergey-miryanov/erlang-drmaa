-module (drmaa).
-author ('sergey.miryanov@gmail.com').
-export ([start/0]).
-export ([test/1]).

%-behaviour (gen_server).

start () ->
  case erl_ddll:load (".", "drmaa_drv")
  of
    ok -> Port = open_port ({spawn, "drmaa_drv"}, [binary]);
    {error, Error} -> io:format ("Error: ~p~n", [erl_ddll:format_error (Error)])
  end.

test (Port) ->
  port_control (Port, 1, <<"xxx">>),
  port_control (Port, 2, <<"xxx">>),
  port_control (Port, 3, <<"xxx">>),
  port_control (Port, 4, <<"xxx">>).
