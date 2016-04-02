-module(mapred_app).

-behaviour(application).

%% Application callbacks
-export([start/0, start/2, stop/1]).

%% API
-export([ping/0]).

%% ===================================================================
%% Application callbacks
%% ===================================================================
start() ->
    application:start(mapred).

start(_StartType, _StartArgs) ->
    mapred_sup:start_link().

stop(_State) ->
    ok.

%% ===================================================================
%% Application callbacks
%% ===================================================================
ping() ->
    pong.
