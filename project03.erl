%%%-------------------------------------------------------------------
%%% @author seanlee
%%% @copyright (C) 2022, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 21. Oct 2022 21:34
%%%-------------------------------------------------------------------
-module(project03).
-author("seanlee").

%% API
-export([main/2,
         createChord/4,
         addNodeToChord/3,
         listenNodeTaskCompletion/1,
         storeNumberOfHops/1,
         startMessageOnAllNodes/3,
         listenForCompletion/4,
         startStabilize/1,
         printAverageHops/1]).
main(NumNodes,NumRequest)->
  ets:new(node_id,[named_table,public]),
  ets:new(tbl,[named_table,public]),
  ets:new(averageHops,[named_table,public]),
  M = nodeActor:log2Ceil(NumNodes),
  Seq = lists:seq(1,nodeActor:pow2Int(M)),
  NodesInChord = createChord([],Seq,M,NumNodes),
  listenForCompletion(NodesInChord,NumNodes,NumRequest,M).


createChord(NodesInChord,_,_M,NumNodes) when NumNodes =< 0 ->
  NodesInChord;

createChord(NodesInChord, RemainingNodes,M,NumNodes) when NumNodes > 0 ->
  New_NodesInChord = addNodeToChord(RemainingNodes,NodesInChord,M),
  createChord(New_NodesInChord,RemainingNodes,M,NumNodes-1).

addNodeToChord(RemainingNodes,NodesInChord,M) ->
  HashVal = nodeActor:randomEnum(RemainingNodes -- NodesInChord),
  Pid = spawn(fun() -> nodeActor:start_link(HashVal,M,NodesInChord) end),
  erlang:monitor(process,Pid),
  [HashVal | NodesInChord].

listenNodeTaskCompletion(NumNodes) when NumNodes =< 1 ->
  io:format("All Nodes have finished their task");
listenNodeTaskCompletion(NumNodes) when NumNodes > 1 ->
  receive
    {completed,Pid,HopsCount,Key,Succ} ->
      storeNumberOfHops(HopsCount),
      io:format("~p found key ~p with Node: ~p in hops ~p",[Pid,Key,Succ,HopsCount]),
      listenNodeTaskCompletion(NumNodes-1)
  after
    15000 ->
      io:format("Deadlock occured, please try again"),
      exit(self(),killed)
  end.

storeNumberOfHops(HopsCount) ->
  case ets:lookup(averageHops, 1) of
    [{_,Hops}] -> ets:insert(averageHops,{1,HopsCount+Hops});
    [] -> ets:insert(averageHops,{1,HopsCount})
  end.

startMessageOnAllNodes(_NodesInChord,0,_M) ->
  ok;
startMessageOnAllNodes(NodesInChord,NumRequest,M) ->
  timer:sleep(1000),
  Key = nodeActor:getRandomKey(M),
  lists:foreach(fun(N) ->gen_server:cast(nodeActor:getPid(N),{lookup,N,Key,0}) end,NodesInChord),
  startMessageOnAllNodes(NodesInChord,NumRequest-1,M).


listenForCompletion(NodesInChord, NumNodes, NumRequest,M) ->
  timer:sleep(2000),
  startStabilize(NodesInChord),
  % not sure
  listenNodeTaskCompletion(NumNodes*NumRequest),
  P = self(),
  ets:insert(node_id,{-1,P}),

  timer:sleep(2000),
  startMessageOnAllNodes(NodesInChord,NumRequest,M),

  List = lists:filter(fun(X)->nodeActor:isAlive(X) end,NodesInChord),
  lists:foreach(fun(X)-> gen_server:call(nodeActor:getPid(X),state) end, List),
  printAverageHops(NumNodes * NumRequest).

startStabilize(NodesInChord) ->
  Node = nodeActor:randomEnum(NodesInChord),
  Pid = nodeActor:getPid(Node),
  case Pid of
    null -> startStabilize(NodesInChord);
    _ -> gen_server:cast(Pid,stabilize)
  end.



printAverageHops(NumNodes) ->
  case ets:first(averageHops) of
    "$end_of_table" -> null;
    key ->
      case ets:lookup(averageHops,key) of
        [] -> null;
        [{_,Value}] ->
          io:format("Average hops for a message:~p~n",[Value/NumNodes]),
          ets:delete(averageHops,key),
          printAverageHops(NumNodes)
      end
  end.


