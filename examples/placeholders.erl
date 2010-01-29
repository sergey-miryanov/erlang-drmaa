#!/usr/bin/env escript
%%! -pa ../ebin -pa ebin -sname placeholder

%-module (attrs).
%-export ([main/1]).

main (_) ->
  drmaa:start_link (),

  PlaceholderHD = drmaa:placeholder (hd),
  PlaceholderHD = drmaa:placeholder (home),
  PlaceholderHD = drmaa:placeholder (home_dir),

  PlaceholderWD = drmaa:placeholder (wd),
  PlaceholderWD = drmaa:placeholder (working_dir),

  PlaceholderIncr = drmaa:placeholder (incr),

  io:format ("~p~n~p~n~p~n", [PlaceholderHD, PlaceholderWD, PlaceholderIncr]),

  ok.
