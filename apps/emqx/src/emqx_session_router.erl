%%--------------------------------------------------------------------
%% Copyright (c) 2017-2021 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-module(emqx_session_router).

-behaviour(gen_server).

-include("emqx.hrl").
-include("logger.hrl").
-include("types.hrl").

%% Mnesia bootstrap
-export([mnesia/1]).

-boot_mnesia({mnesia, [boot]}).
-copy_mnesia({mnesia, [copy]}).

-export([ create_init_tab/0
        , start_link/2]).

%% Route APIs
-export([ do_add_route/2
        , do_delete_route/2
        , match_routes/1
        ]).

-export([ abandon/1
        , delivered/2
        , persist/1
        , pending/1
        , pending/2
        , resume_begin/1
        , resume_begin_rpc_endpoint/2 %% Only for internal rpc
        , resume_end/1
        , resume_end_rpc_endpoint/2   %% Only for internal rpc
        ]).

-export([print_routes/1]).

%% gen_server callbacks
-export([ init/1
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , terminate/2
        , code_change/3
        ]).

-type(group() :: binary()).

-type(dest() :: node() | {group(), node()}).

-define(ROUTE_TAB, emqx_session_route).
-define(SESS_MSG_TAB, emqx_session_msg).
-define(MSG_TAB, emqx_persistent_msg).

%% NOTE: Order is significant because of traversal order of the table.
-define(MARKER, 3).
-define(ABANDONED, 2).
-define(DELIVERED, 1).
-define(UNDELIVERED, 0).
-type pending_tag() :: ?DELIVERED | ?UNDELIVERED | ?ABANDONED | ?MARKER.
-record(session_msg, {key      :: {binary(), emqx_guid:guid(), pending_tag()},
                      val = [] :: []}).

-define(SESSION_INIT_TAB, session_init_tab).

%%--------------------------------------------------------------------
%% Mnesia bootstrap
%%--------------------------------------------------------------------

mnesia(boot) ->
    ok = ekka_mnesia:create_table(?ROUTE_TAB, [
                {type, bag},
                {rlog_shard, ?ROUTE_SHARD},
                {ram_copies, [node()]},
                {record_name, route},
                {attributes, record_info(fields, route)},
                {storage_properties, [{ets, [{read_concurrency, true},
                                             {write_concurrency, true}]}]}]),
    ok = ekka_mnesia:create_table(?SESS_MSG_TAB, [
                {type, ordered_set},
                {rlog_shard, ?PERSISTENT_SESSION_SHARD},
                {ram_copies, [node()]},
                {record_name, session_msg},
                {attributes, record_info(fields, session_msg)},
                {storage_properties, [{ets, [{read_concurrency, true},
                                             {write_concurrency, true}]}]}]),
    %% TODO: This should be external
    ok = ekka_mnesia:create_table(?MSG_TAB, [
                {type, set},
                {rlog_shard, ?PERSISTENT_SESSION_SHARD},
                {ram_copies, [node()]},
                {record_name, message},
                {attributes, record_info(fields, message)},
                {storage_properties, [{ets, [{read_concurrency, true},
                                             {write_concurrency, true}]}]}]);
mnesia(copy) ->
    ok = ekka_mnesia:copy_table(?ROUTE_TAB, ram_copies),
    ok = ekka_mnesia:copy_table(?SESS_MSG_TAB, ram_copies),
    %% TODO: This should be external
    ok = ekka_mnesia:copy_table(?MSG_TAB, ram_copies).

%%--------------------------------------------------------------------
%% Start a router
%%--------------------------------------------------------------------

create_init_tab() ->
    emqx_tables:new(?SESSION_INIT_TAB, [public, {read_concurrency, true},
                                        {write_concurrency, true}]).

-spec(start_link(atom(), pos_integer()) -> startlink_ret()).
start_link(Pool, Id) ->
    gen_server:start_link({local, emqx_misc:proc_name(?MODULE, Id)},
                          ?MODULE, [Pool, Id], [{hibernate_after, 1000}]).

%%--------------------------------------------------------------------
%% Route APIs
%%--------------------------------------------------------------------

-spec(do_add_route(emqx_topic:topic(), dest()) -> ok | {error, term()}).
do_add_route(Topic, SessionID) when is_binary(Topic) ->
    Route = #route{topic = Topic, dest = SessionID},
    case lists:member(Route, lookup_routes(Topic)) of
        true  -> ok;
        false ->
            case emqx_topic:wildcard(Topic) of
                true  ->
                    Fun = fun emqx_router_utils:insert_trie_route/2,
                    emqx_router_utils:maybe_trans(Fun, [?ROUTE_TAB, Route],
                                                  ?PERSISTENT_SESSION_SHARD);
                false ->
                    emqx_router_utils:insert_direct_route(?ROUTE_TAB, Route)
            end
    end.

%% @doc Match routes
-spec(match_routes(emqx_topic:topic()) -> [emqx_types:route()]).
match_routes(Topic) when is_binary(Topic) ->
    case match_trie(Topic) of
        [] -> lookup_routes(Topic);
        Matched ->
            lists:append([lookup_routes(To) || To <- [Topic | Matched]])
    end.

%% Optimize: routing table will be replicated to all router nodes.
match_trie(Topic) ->
    case emqx_trie:empty_session() of
        true -> [];
        false -> emqx_trie:match_session(Topic)
    end.

-spec(do_delete_route(emqx_topic:topic(), dest()) -> ok | {error, term()}).
do_delete_route(Topic, SessionID) ->
    Route = #route{topic = Topic, dest = SessionID},
    case emqx_topic:wildcard(Topic) of
        true  ->
            Fun = fun emqx_router_utils:delete_trie_route/2,
            emqx_router_utils:maybe_trans(Fun, [?ROUTE_TAB, Route], ?PERSISTENT_SESSION_SHARD);
        false ->
            emqx_router_utils:delete_direct_route(?ROUTE_TAB, Route)
    end.

%% @doc Print routes to a topic
-spec(print_routes(emqx_topic:topic()) -> ok).
print_routes(Topic) ->
    lists:foreach(fun(#route{topic = To, dest = SessionID}) ->
                      io:format("~s -> ~s~n", [To, SessionID])
                  end, match_routes(Topic)).

%%--------------------------------------------------------------------
%% Message APIs
%%--------------------------------------------------------------------

persist(Msg) ->
    case emqx_message:get_flag(dup, Msg) orelse emqx_message:is_sys(Msg) of
        true  -> ok;
        false ->
            case match_routes(emqx_message:topic(Msg)) of
                [] -> ok;
                Routes ->
                    %% TODO: This should store in external backend
                    ekka_mnesia:dirty_write(?MSG_TAB, Msg),
                    MsgId = emqx_message:id(Msg),
                    Fun = fun(#route{dest = SessionID}) ->
                                  Key = {SessionID, MsgId, ?UNDELIVERED},
                                  ekka_mnesia:dirty_write(?SESS_MSG_TAB, #session_msg{ key = Key }),
                                  case emqx_tables:lookup_value(?SESSION_INIT_TAB, SessionID) of
                                      undefined -> ok;
                                      Worker -> Worker ! {deliver, Msg}
                                  end
                          end,
                    lists:foreach(Fun, Routes)
            end
    end.

delivered(SessionID, MsgIDs) ->
    Fun = fun(MsgID) ->
                  Key = {SessionID, MsgID, ?DELIVERED},
                  ekka_mnesia:dirty_write(?SESS_MSG_TAB, #session_msg{ key = Key })
          end,
    lists:foreach(Fun, MsgIDs).

%%--------------------------------------------------------------------
%% Session APIs
%%--------------------------------------------------------------------

abandon(Session) ->
    SessionID = emqx_session:info(id, Session),
    Subscriptions = emqx_session:info(subscriptions, Session),
    cast(pick(SessionID), {abandon, SessionID, Subscriptions}).

pending(SessionID) ->
    call(pick(SessionID), {pending, SessionID}).

-spec pending(emqx_session:sessionID(), [emqx_guid:guid()]) ->
          [emqx_types:message()] | 'incomplete'.
pending(SessionID, MarkerIDs) ->
    call(pick(SessionID), {pending, SessionID, MarkerIDs}).


-spec resume_begin(binary()) -> [{node(), emqx_guid:guid()}].
resume_begin(SessionID) ->
    Nodes = ekka_mnesia:running_nodes(),
    Res = erpc:multicall(Nodes, ?MODULE, resume_begin_rpc_endpoint, [self(), SessionID]),
    [{Node, Marker} || {{ok, {ok, Marker}}, Node} <- lists:zip(Nodes, Res)].

-spec resume_begin_rpc_endpoint(pid(), binary()) -> [{node(), emqx_guid:guid()}].
resume_begin_rpc_endpoint(From, SessionID) when is_pid(From), is_binary(SessionID) ->
    call(pick(SessionID), {resume_begin, From, SessionID}).

-spec resume_end(binary()) -> [emqx_types:message()].
resume_end(SessionID) ->
    Nodes = ekka_mnesia:running_nodes(),
    Res = erpc:multicall(Nodes, ?MODULE, resume_end_rpc_endpoint, [self(), SessionID]),
    %% TODO: Should handle the errors
    lists:append([ Messages || {ok, Messages} <- Res]).

-spec resume_end_rpc_endpoint(pid(), binary()) ->
          {'ok', [emqx_types:message()]} | {'error', term()}.
resume_end_rpc_endpoint(From, SessionID) when is_pid(From), is_binary(SessionID) ->
    case emqx_tables:lookup_value(?SESSION_INIT_TAB, SessionID) of
        undefined ->
            {error, not_found};
        Pid ->
            Res = emq_session_router_worker:resume_end(From, Pid),
            cast(pick(SessionID), {resume_end, SessionID, Pid}),
            Res
    end.

%%--------------------------------------------------------------------
%% Worker internals
%%--------------------------------------------------------------------

call(Router, Msg) ->
    gen_server:call(Router, Msg, infinity).

cast(Router, Msg) ->
    gen_server:cast(Router, Msg).

pick(#route{dest = SessionID}) ->
    gproc_pool:pick_worker(session_router_pool, SessionID);
pick(SessionID) when is_binary(SessionID) ->
    gproc_pool:pick_worker(session_router_pool, SessionID).

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([Pool, Id]) ->
    true = gproc_pool:connect_worker(Pool, {Pool, Id}),
    {ok, #{pool => Pool, id => Id, pmon => emqx_pmon:new()}}.

handle_call({resume_begin, RemotePid, SessionID}, _From, State) ->
    case init_resume_worker(RemotePid, SessionID, State) of
        error ->
            {reply, error, State};
        {ok, Pid, State1} ->
            MarkerID = emqx_guid:gen(),
            ets:insert(?SESSION_INIT_TAB, {SessionID, Pid}),
            DBKey = {SessionID, MarkerID, ?MARKER},
            ekka_mnesia:dirty_write(?SESS_MSG_TAB, #session_msg{ key = DBKey }),
            {reply, {ok, MarkerID}, State1}
    end;
handle_call({pending, SessionID}, _From, State) ->
    {reply, pending_messages(SessionID, []), State};
handle_call({pending, SessionID, MarkerIDs}, _From, State) ->
    {reply, pending_messages(SessionID, MarkerIDs), State};
handle_call(Req, _From, State) ->
    ?LOG(error, "Unexpected call: ~p", [Req]),
    {reply, ignored, State}.

handle_cast({abandon, SessionID, Subscriptions}, State) ->
    Key = {SessionID, <<>>, ?ABANDONED},
    ekka_mnesia:dirty_write(?SESS_MSG_TAB, #session_msg{ key = Key }),
    %% TODO: Make a batch for deleting all routes.
    Fun = fun(Topic, _) -> do_delete_route(Topic, SessionID) end,
    ok = maps:foreach(Fun, Subscriptions),
    {noreply, State};
handle_cast({resume_end, SessionID, Pid}, State) ->
    case emqx_tables:lookup_value(?SESSION_INIT_TAB, SessionID) of
        undefined -> skip;
        {_, P} when P =:= Pid -> ets:delete(?SESSION_INIT_TAB, SessionID);
        {_,_P} -> skip
    end,
    Pmon = emqx_pmon:demonitor(Pid, maps:get(pmon, State)),
    emqx_session_router_sup:abort_worker(Pid),
    {noreply, State#{ pmon => Pmon }};
handle_cast(Msg, State) ->
    ?LOG(error, "Unexpected cast: ~p", [Msg]),
    {noreply, State}.

handle_info(Info, State) ->
    ?LOG(error, "Unexpected info: ~p", [Info]),
    {noreply, State}.

terminate(_Reason, #{pool := Pool, id := Id}) ->
    gproc_pool:disconnect_worker(Pool, {Pool, Id}).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% Resume worker. A process that buffers the persisted messages during
%% initialisation of a resuming session.
%%--------------------------------------------------------------------

init_resume_worker(RemotePid, SessionID, #{ pmon := Pmon } = State) ->
    case emqx_session_router_worker_sup:start_worker(SessionID, RemotePid) of
        {error, What} ->
            ?SLOG(error, #{msg => "Could not start resume worker", reason => What}),
            error;
        {ok, Pid} ->
            Pmon1 = emqx_pmon:monitor(Pid, Pmon),
            case emqx_tables:lookup_value(?SESSION_INIT_TAB, SessionID) of
                undefined ->
                    {ok, Pid, State#{ pmon => Pmon1 }};
                {_, OldPid} ->
                    Pmon2 = emqx_pmon:demonitor(OldPid, Pmon1),
                    emqx_session_router_sup:abort_worker(OldPid),
                    {ok, Pid, State#{ pmon => Pmon2 }}
            end
    end.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

lookup_routes(Topic) ->
    ets:lookup(?ROUTE_TAB, Topic).

pending_messages(SessionID, MarkerIds) ->
    %% TODO: The reading of messages should be from external DB
    Fun = fun() ->
                  case pending_messages(SessionID, <<>>, ?DELIVERED, [], MarkerIds) of
                      {Pending, []} ->
                          [hd(mnesia:read(?MSG_TAB, MsgId))
                           || MsgId <- Pending];
                      {_Pending, [_|_]} ->
                          incomplete
                  end
          end,
    {atomic, Msgs} = ekka_mnesia:ro_transaction(?PERSISTENT_SESSION_SHARD, Fun),
    Msgs.

%% The keys are ordered by
%%     {sessionID(), <<>>, ?ABANDONED} For abandoned sessions (clean started or expired).
%%     {sessionID(), emqx_guid:guid(), ?DELIVERED | ?UNDELIVERED | ?MARKER}
%%  where
%%     <<>> < emqx_guid:guid()
%%     emqx_guid:guid() is ordered in ts() and by node()
%%     ?UNDELIVERED < ?DELIVERED < ?MARKER
%%
%% We traverse the table until we reach another session.
%% TODO: Garbage collect the delivered messages.
pending_messages(SessionID, PrevMsgId, PrevTag, Acc, MarkerIds) ->
    %% ct:pal("MarkerIds: ~p", [MarkerIds]),
    case mnesia:dirty_next(?SESS_MSG_TAB, {SessionID, PrevMsgId, PrevTag}) of
        {S, <<>>, ?ABANDONED} when S =:= SessionID ->
            {[], []};
        {S, MsgId, ?MARKER = Tag} when S =:= SessionID ->
            MarkerIds1 = MarkerIds -- [MsgId],
            case PrevTag of
                ?DELIVERED   -> pending_messages(SessionID, MsgId, Tag, Acc, MarkerIds1);
                ?MARKER      -> pending_messages(SessionID, MsgId, Tag, Acc, MarkerIds1);
                ?UNDELIVERED -> pending_messages(SessionID, MsgId, Tag, [PrevMsgId|Acc], MarkerIds1)
            end;
        {S, MsgId, Tag} = Key when S =:= SessionID, MsgId =:= PrevMsgId ->
            Tag =:= ?UNDELIVERED andalso error({assert_fail}, Key),
            pending_messages(SessionID, MsgId, Tag, Acc, MarkerIds);
        {S, MsgId, Tag} when S =:= SessionID ->
            case PrevTag of
                ?DELIVERED   -> pending_messages(SessionID, MsgId, Tag, Acc, MarkerIds);
                ?MARKER      -> pending_messages(SessionID, MsgId, Tag, Acc, MarkerIds);
                ?UNDELIVERED -> pending_messages(SessionID, MsgId, Tag, [PrevMsgId|Acc], MarkerIds)
            end;
        _What -> %% Next sessionID or '$end_of_table'
            case PrevTag of
                ?DELIVERED   -> {lists:reverse(Acc), MarkerIds};
                ?MARKER      -> {lists:reverse(Acc), MarkerIds};
                ?UNDELIVERED -> {lists:reverse([PrevMsgId|Acc]), MarkerIds}
            end
    end.
