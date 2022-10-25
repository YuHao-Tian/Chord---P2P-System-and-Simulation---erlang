%%%-------------------------------------------------------------------
%%% @author seanlee
%%% @copyright (C) 2022, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 22. Oct 2022 16:17
%%%-------------------------------------------------------------------
-module(nodeActor).
-author("seanlee").

%% API
-export([start_link/3,
         init/1,
         handle_call/3,
         handle_cast/2,
         predecessorFailed/1,
         findSuccessor/4,
         lookupSuccessor/4,
         lookupOfClosest/5,
         getSuccessorOfClosest/5,
         notify/2,
         lookup/4,
         sendCompletion/4,
         closestTo/2,
         set_nth/4,
         find/2,
         getPid/1,
         getSuccessorFromState/1,
         isAlive/1,
         notAlive/1,
         getPredecessor/1,
         randomNode/2,
         default/2,
         isInRange/4,
         getValidNextId/2,
         randomEnum/1,
         getRandomKey/1,
         pow2Int/1,
         log2Ceil/1,
         getHash/1,
         stabilize_start/1,
         stabilize/1,
         notifySuccessorToUpdatePredecessor/2,
         fixFinger/2,
         checkPredecessor/1,
         startOnOtherNodes/2,
         nextPid/2,
         updateSuccessor/2,
         updateSuccessor/3
         ]).

start_link(Node_id,M,ExistingNodes) ->
  Pid = gen_server:start_link(?MODULE,[Node_id,M,ExistingNodes],[]),
  ets:insert(node_id,{Node_id,Pid}).

init([Node_id,M,ExistingNodes]) ->
  FingerTable = lists:duplicate(M,randomNode(Node_id,ExistingNodes)),
  State = #{
    id=>Node_id,
    predecessor => null,
    finger_table => FingerTable,
    next => 0,
    m => M
  },
  {ok,State}.

handle_call(Request,_From,State)->
  case Request of
    state ->
      {reply,State,State};
    {findSuccessor,Origin,Node_id,HopsCount} ->
      {reply,findSuccessor(Origin,Node_id,HopsCount,State),State}
  end.

handle_cast(Request,State) ->
    case Request of
      {notify, Node_id} -> New_state = notify(Node_id,State);
      {lookup, Origin, Key, HopsCount} -> New_state =lookup(Origin,Key,HopsCount,State);
      stabilize -> New_state = stabilize_start(State)
    end,
  {noreply,New_state}.


predecessorFailed(State) ->
  #{predecessor:=P} = State,
  R = notAlive(P),
  if
    P =/= null orelse R =:= true -> true;
    true -> false
  end.

findSuccessor(Origin,Node_id,HopsCount,State) ->
  Successor = getSuccessorFromState(State),
  #{id := From, m := M} = State,
  Res = isInRange(From,Node_id,Successor+1,M),
  case Res of
    true -> [Successor,HopsCount];
    false ->
      Closest = closestTo(Node_id,State),
      getSuccessorOfClosest(Closest,From,Node_id,HopsCount+1,Origin)
  end.

lookupSuccessor(Origin,Node_id,HopsCount,State) ->
  #{id := From, m := M} = State,
  Successor = getSuccessorFromState(State),
  Res = isInRange(From,Node_id,Successor+1,M),
  case Res of
    true -> sendCompletion(Origin,HopsCount,Node_id,Successor);
    false ->
      Closest = closestTo(Node_id,State),
      lookupOfClosest(Closest,From,Node_id,HopsCount+1,Origin)
  end.

lookupOfClosest(Closest,ID,Node_id,HopsCount,Origin)->
  R = notAlive(Closest),
  if
    Closest =:= ID orelse R =:= true orelse Origin =:= Closest  ->
      sendCompletion(Origin,HopsCount,Node_id,Closest);
    true ->
      Pid = getPid(Closest),
      gen_server:cast(Pid,{lookup,Origin,Node_id,HopsCount})
  end.


getSuccessorOfClosest(Closest,ID,Node_id,HopsCount,Origin) ->
  R = notAlive(Closest),
  if
    Closest =:= ID orelse R =:= true orelse Origin =:= Closest  -> [ID,HopsCount];
    true ->
      Pid = getPid(Closest),
      gen_server:call(Pid,{findSuccessor,Origin,Node_id,HopsCount})
  end.

notify(Node_id,State)->
  #{id:=ID,predecessor:=P,m:=M}= State,
  R = isInRange(P,Node_id,ID,M),
  if
    P =:= null orelse R =:= true ->
      ets:insert(tbl,{ID,Node_id}),
      State#{predecessor := Node_id};
    true -> State
  end.

lookup(Origin,Key,HopsCount,State)->
  #{id:=ID}= State,
  if
    Key =:= ID -> sendCompletion(Origin,HopsCount,Key,Key);
    true -> lookupSuccessor(Origin,Key,HopsCount,State)
  end,
  State.

sendCompletion(Id,HopsCount,Key,Succ)->
  [_,Pid]= ets:lookup(node_id,-1),
  erlang:send(Pid,{completed,Id,HopsCount,Key,Succ}).


closestTo(Node_id,State)->
  #{id := From, finger_table := Finger_table, m := M} = State,
  Finger_table_entry_list= lists:reverse(lists:droplast(Finger_table)),
  Finger_table_entry = find(fun(E)->isInRange(From,E,Node_id,M) end,Finger_table_entry_list),
  default(Finger_table_entry,From).


%% actorUtils
set_nth(New_list,[_H|T],1,Element) ->
%%  io:format("New_list~p~n",[New_list]),
%%  io:format("T:~p~n",[T]),

  lists:reverse([Element|New_list]) ++ T;
set_nth(New_list,[H|T],Index,Element) ->
  L = erlang:length([H|T]),
  if
    L < Index -> [H|T];
    true -> set_nth([H|New_list],T,Index-1,Element)
  end.



find(F,[H|T]) ->
  case F(H) of
    true -> H;
    false -> find(F,T)
  end;
find(F,[]) when is_function(F,1) -> [].

getPid(Node_id) ->
  case ets:lookup(node_id,Node_id) of
    [] -> null;
    [{_,{_,Pid}}] -> Pid
  end.


getSuccessorFromState(State)->
  #{id := ID,finger_table:=Finger_table} = State,
  S = lists:nth(1,Finger_table),
  R = isAlive(S),
  case R of
    true -> S;
    false -> ID
  end.

isAlive(Id) ->
  Res = getPid(Id),
  case Res of
    null -> false;
    _ -> true
  end.

notAlive(Id) ->
  Res = getPid(Id),
  case Res of
    null -> true;
    _ -> false
  end.

getPredecessor(Id)->
  case ets:lookup(tbl,Id) of
    [{_,P}] -> P;
    [] -> null
  end.


randomNode(Node_id,[]) ->
  Node_id;

randomNode(_Node_id,ExistingNodes) ->
  randomEnum(ExistingNodes).

default(Key,DefaultValue) ->
  if
    Key =:= null -> DefaultValue;
    true ->
      R = isAlive(Key),
      case R of
        true -> Key;
        false -> DefaultValue
      end
  end.


%% mathUtils
isInRange(From,Key,To,M) ->
  Total = pow2Int(M),
  if
    To > Total + 1 -> New_To = 1;
    true -> New_To = To
  end,
  if
    From < New_To -> (From < Key) andalso (Key < New_To);
    From =:= New_To -> Key =/= From;
    true -> ((Key > 0) andalso (Key < New_To)) orelse ((Key > From) andalso (Key =< Total))
  end.


getValidNextId(Next,State) ->
  #{id:=ID,m:=M} = State,
  NumNodes = pow2Int(M),
  NextID = ID  + pow2Int(Next),
  if
    NextID > NumNodes -> NextID - NumNodes;
    true -> NextID
  end.

randomEnum(List) ->
  L = erlang:length(List),
  R = rand:uniform(L),
  lists:nth(R,List).

getRandomKey(M) ->
  List = lists:seq(1,pow2Int(M)),
  randomEnum(List).

pow2Int(K) ->
  trunc(math:pow(2,K)).


log2Ceil(NumNodes)  ->
  trunc(math:ceil(math:log2(NumNodes))).

getHash(N) ->
  S = "node" ++ integer_to_list(N),
  Temp1 = crypto:hash(sha256,S),
  %% transform from binary to hex
  Temp2 = binary:encode_hex(Temp1),
  Temp3 = binary:bin_to_list(Temp2),
  %% transform from hex to decimal
  Hash = erlang:list_to_integer(Temp3,16),
  Hash band log2Ceil(N).


%% stabilize
stabilize_start(State)->
  New_State1 = stabilize(State),
  #{id:=ID1} = New_State1,
  New_State2 = fixFinger(New_State1,ID1),
  New_State3 = checkPredecessor(New_State2),
  #{id:=ID2,m:=M} = New_State3,
  startOnOtherNodes(ID2+1,M),
  New_State3.

stabilize(State) ->
  #{id := ID, predecessor:=P,m := M} = State,
  Successor = getSuccessorFromState(State),
  Predecessor = case ID of
                  Successor -> P;
                  _ -> getPredecessor(Successor)
                end,
  R = isInRange(ID,P,Successor,M),
  if
    Predecessor =/= null andalso R=:= true -> New_Successor = Predecessor;
    true -> New_Successor = Successor
  end,

  notifySuccessorToUpdatePredecessor(New_Successor,ID),
  updateSuccessor(New_Successor,State).

notifySuccessorToUpdatePredecessor(Successor,NewPredecessor) ->
  Pid = getPid(Successor),
  gen_server:cast(Pid,{notify,NewPredecessor}).

fixFinger(State,Origin) ->
  #{next := Next,m:=M} = State,
  NextID = getValidNextId(Next,State),
  [Successor,_HopsCount] = findSuccessor(Origin,NextID,0,State),
  New_State = updateSuccessor(Successor,State,NextID),

  New_State#{next:= (Next+1) rem M}.

checkPredecessor(State) ->
  R = predecessorFailed(State),
  case R of
    true -> State#{predecessor := null};
    false -> State
  end.

startOnOtherNodes(ID,M) ->
  Pid = nextPid(ID, M),
  case Pid of
    null -> startOnOtherNodes(ID+1,M);
    _ -> gen_server:cast(Pid,stabilize)
  end.

nextPid(ID,M) ->
  R = pow2Int(M),
  if
     ID > R ->getPid(1);
    true -> getPid(ID)
  end.

updateSuccessor(Successor,State) ->
  #{finger_table := Finger_table} = State,
  FT = set_nth([],Finger_table,1,Successor),
  State#{finger_table := FT}.

updateSuccessor(Successor,State,Index) ->
  #{finger_table := Finger_table} = State,
  FT = set_nth([],Finger_table,Index,Successor),
  State#{finger_table := FT}.


