-module(wc).

-export([test/0,
	 wc/1]).

test() ->
    {ok, Root} = file:get_cwd(),
    wc(Root ++ "/src").

wc(Dir) ->
    Map = fun emit_words/2,
    Reduce = fun count_words/3,
    FileNames = files(Dir),
    Result = mapred:mapreduce(Map, Reduce, [], FileNames),
    lists:reverse(lists:sort(Result)).

files(Dir)
  when is_list(Dir) ->
    {ok, Files} = file:list_dir(Dir),
    lists:map(fun(File) -> Dir ++ "/" ++ File end, Files).

emit_words(Pid, File) ->
    {ok, Handle} = file:open(File, [read]),
    read_words(Pid, Handle, io:scan_erl_exprs(Handle, " ")).

read_words(Pid, Handle, {ok, Data, _}) ->
    scan_words(Pid, Data),
    read_words(Pid, Handle, io:scan_erl_exprs(Handle, " "));
read_words(_Pid, Handle, _) ->
    file:close(Handle).

scan_words(_Pid, []) -> ok;
scan_words(Pid, [{atom, _, Word}|Tail]) ->
    Pid ! {self(), Word, 1},
    scan_words(Pid, Tail);
scan_words(Pid, [_|Tail]) ->
    scan_words(Pid, Tail).

count_words(Key, Values, Acc) ->
    [{length(Values), Key} | Acc].
