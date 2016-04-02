-module(mapred).

-behaviour(gen_server).

%% API
-export([start_link/0,
	 mapreduce/4]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
	 terminate/2, code_change/3]).

-define(SERVER, ?MODULE).

-record(state, {
	  caller :: pid(),
	  map :: function(),
	  reduce :: function(),
	  accum :: term(),
	  buffer = #{} :: map(),
	  mappers = #{} :: map()
	 }).

%%%===================================================================
%%% API
%%%===================================================================
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

mapreduce(Map, Reduce, Accum, Input) ->
    From = self(),
    gen_server:cast(?SERVER, {start, From, Map, Reduce, Accum, Input}),
    receive
	{result, Result} ->
	    Result
    after 5000 ->
	    timeout
    end.

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================
init([]) ->
    {ok, #state{}}.

handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

handle_cast({start, From, Map, Reduce, Accum, Input}, State=#state{}) ->
    {ok, Mappers} = spawn_mappers(Map, Input),
    {noreply, State#state{caller=From, map=Map, reduce=Reduce,
			  accum=Accum, mappers=Mappers}};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({From, Key, Val}, State=#state{buffer=Buffer0, mappers=Mappers}) ->
    case maps:find(From, Mappers) of
	{ok, _MRef} ->
	    {noreply, State#state{buffer=buffer(Key, Val, Buffer0)}};
	error ->
	    {noreply, State}
    end;
handle_info({'DOWN', MRef, process, Pid, _Reason},
	    State=#state{mappers=Mappers0}) ->
    case maps:find(Pid, Mappers0) of
	{ok, MRef} ->
	    Mappers=maps:remove(Pid, Mappers0),
	    maybe_reduce(Mappers),
	    {noreply, State#state{mappers=Mappers}};
	error ->
	    {noreply, State}
    end;
handle_info(reduce, #state{caller=Caller,
			   reduce=Reduce,
			   accum=Accum,
			   buffer=Buffer}) ->
    Result = maps:fold(Reduce, Accum, Buffer),
    Caller ! {result, Result},
    {noreply, #state{}};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
-spec spawn_mappers(function(), list()) -> {ok, map()} | {error, term()}.
spawn_mappers(Map, Input) ->
    Pid = self(),
    {ok, lists:foldl(fun(Elem, Acc) ->
			     MPid = spawn(fun() -> Map(Pid, Elem) end),
			     maps:put(MPid, monitor(process, MPid), Acc)
		     end, #{}, Input)}.

-spec buffer(term(), term(), map()) -> map().
buffer(Key, Value, Buffer) ->
    KeyBuff = maps:get(Key, Buffer, []),
    maps:put(Key, [Value|KeyBuff], Buffer).

-spec maybe_reduce(map()) -> void.
maybe_reduce(Map)
  when is_map(Map) ->
    case maps:size(Map) == 0 of
	true -> self() ! reduce;
	false -> void
    end;
maybe_reduce(_) -> void.
