%%--------------------------------------------------------------------
%% Copyright (c) 2021-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_persistent_session_ds).

-behaviour(emqx_session).

-include("emqx.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("stdlib/include/ms_transform.hrl").

-include("emqx_mqtt.hrl").

-include("emqx_persistent_session_ds.hrl").

%% Session API
-export([
    create/3,
    open/2,
    destroy/1
]).

-export([
    info/2,
    stats/1
]).

-export([
    subscribe/3,
    unsubscribe/2,
    get_subscription/2
]).

-export([
    publish/3,
    puback/3,
    pubrec/2,
    pubrel/2,
    pubcomp/3
]).

-export([
    deliver/3,
    replay/3,
    handle_timeout/3,
    disconnect/1,
    terminate/2
]).

%% session table operations
-export([create_tables/0]).

%% Remove me later (satisfy checks for an unused BPAPI)
-export([
    do_open_iterator/3,
    do_ensure_iterator_closed/1,
    do_ensure_all_iterators_closed/1
]).

-ifdef(TEST).
-export([
    session_open/1,
    list_all_sessions/0,
    list_all_subscriptions/0,
    list_all_streams/0,
    list_all_iterators/0
]).
-endif.

%% Currently, this is the clientid.  We avoid `emqx_types:clientid()' because that can be
%% an atom, in theory (?).
-type id() :: binary().
-type topic_filter() :: emqx_ds:topic_filter().
-type subscription_id() :: {id(), topic_filter()}.
-type subscription() :: #{
    start_time := emqx_ds:time(),
    propts := map(),
    extra := map()
}.
-type session() :: #{
    %% Client ID
    id := id(),
    %% When the session was created
    created_at := timestamp(),
    %% When the session should expire
    expires_at := timestamp() | never,
    %% Client’s Subscriptions.
    iterators := #{topic() => subscription()},
    %% Inflight messages
    inflight := emqx_persistent_message_ds_replayer:inflight(),
    %%
    props := map()
}.

-type timestamp() :: emqx_utils_calendar:epoch_millisecond().
-type topic() :: emqx_types:topic().
-type clientinfo() :: emqx_types:clientinfo().
-type conninfo() :: emqx_session:conninfo().
-type replies() :: emqx_session:replies().

-export_type([id/0]).

%%

-spec create(clientinfo(), conninfo(), emqx_session:conf()) ->
    session().
create(#{clientid := ClientID}, _ConnInfo, Conf) ->
    % TODO: expiration
    ensure_timers(),
    ensure_session(ClientID, Conf).

-spec open(clientinfo(), conninfo()) ->
    {_IsPresent :: true, session(), []} | false.
open(#{clientid := ClientID}, _ConnInfo) ->
    %% NOTE
    %% The fact that we need to concern about discarding all live channels here
    %% is essentially a consequence of the in-memory session design, where we
    %% have disconnected channels holding onto session state. Ideally, we should
    %% somehow isolate those idling not-yet-expired sessions into a separate process
    %% space, and move this call back into `emqx_cm` where it belongs.
    ok = emqx_cm:discard_session(ClientID),
    case open_session(ClientID) of
        Session = #{} ->
            ensure_timers(),
            {true, Session, []};
        false ->
            false
    end.

ensure_session(ClientID, Conf) ->
    {ok, Session, #{}} = session_ensure_new(ClientID, Conf),
    Session#{iterators => #{}}.

open_session(ClientID) ->
    case session_open(ClientID) of
        {ok, Session, Subscriptions} ->
            Session#{iterators => prep_subscriptions(Subscriptions)};
        false ->
            false
    end.

prep_subscriptions(Subscriptions) ->
    maps:fold(
        fun(Topic, Subscription, Acc) -> Acc#{emqx_topic:join(Topic) => Subscription} end,
        #{},
        Subscriptions
    ).

-spec destroy(session() | clientinfo()) -> ok.
destroy(#{id := ClientID}) ->
    destroy_session(ClientID);
destroy(#{clientid := ClientID}) ->
    destroy_session(ClientID).

destroy_session(ClientID) ->
    session_drop(ClientID).

%%--------------------------------------------------------------------
%% Info, Stats
%%--------------------------------------------------------------------

info(Keys, Session) when is_list(Keys) ->
    [{Key, info(Key, Session)} || Key <- Keys];
info(id, #{id := ClientID}) ->
    ClientID;
info(clientid, #{id := ClientID}) ->
    ClientID;
info(created_at, #{created_at := CreatedAt}) ->
    CreatedAt;
info(is_persistent, #{}) ->
    true;
info(subscriptions, #{iterators := Iters}) ->
    maps:map(fun(_, #{props := SubOpts}) -> SubOpts end, Iters);
info(subscriptions_cnt, #{iterators := Iters}) ->
    maps:size(Iters);
info(subscriptions_max, #{props := Conf}) ->
    maps:get(max_subscriptions, Conf);
info(upgrade_qos, #{props := Conf}) ->
    maps:get(upgrade_qos, Conf);
% info(inflight, #sessmem{inflight = Inflight}) ->
%     Inflight;
% info(inflight_cnt, #sessmem{inflight = Inflight}) ->
%     emqx_inflight:size(Inflight);
% info(inflight_max, #sessmem{inflight = Inflight}) ->
%     emqx_inflight:max_size(Inflight);
info(retry_interval, #{props := Conf}) ->
    maps:get(retry_interval, Conf);
% info(mqueue, #sessmem{mqueue = MQueue}) ->
%     MQueue;
% info(mqueue_len, #sessmem{mqueue = MQueue}) ->
%     emqx_mqueue:len(MQueue);
% info(mqueue_max, #sessmem{mqueue = MQueue}) ->
%     emqx_mqueue:max_len(MQueue);
% info(mqueue_dropped, #sessmem{mqueue = MQueue}) ->
%     emqx_mqueue:dropped(MQueue);
info(next_pkt_id, #{}) ->
    _PacketId = 'TODO';
% info(awaiting_rel, #sessmem{awaiting_rel = AwaitingRel}) ->
%     AwaitingRel;
% info(awaiting_rel_cnt, #sessmem{awaiting_rel = AwaitingRel}) ->
%     maps:size(AwaitingRel);
info(awaiting_rel_max, #{props := Conf}) ->
    maps:get(max_awaiting_rel, Conf);
info(await_rel_timeout, #{props := Conf}) ->
    maps:get(await_rel_timeout, Conf).

-spec stats(session()) -> emqx_types:stats().
stats(Session) ->
    % TODO: stub
    info([], Session).

%%--------------------------------------------------------------------
%% Client -> Broker: SUBSCRIBE / UNSUBSCRIBE
%%--------------------------------------------------------------------

-spec subscribe(topic(), emqx_types:subopts(), session()) ->
    {ok, session()} | {error, emqx_types:reason_code()}.
subscribe(
    TopicFilter,
    SubOpts,
    Session = #{id := ID, iterators := Iters}
) when is_map_key(TopicFilter, Iters) ->
    Iterator = maps:get(TopicFilter, Iters),
    NIterator = update_subscription(TopicFilter, Iterator, SubOpts, ID),
    {ok, Session#{iterators := Iters#{TopicFilter => NIterator}}};
subscribe(
    TopicFilter,
    SubOpts,
    Session = #{id := ID, iterators := Iters}
) ->
    % TODO: max_subscriptions
    Iterator = add_subscription(TopicFilter, SubOpts, ID),
    {ok, Session#{iterators := Iters#{TopicFilter => Iterator}}}.

-spec unsubscribe(topic(), session()) ->
    {ok, session(), emqx_types:subopts()} | {error, emqx_types:reason_code()}.
unsubscribe(
    TopicFilter,
    Session = #{id := ID, iterators := Iters}
) when is_map_key(TopicFilter, Iters) ->
    Iterator = maps:get(TopicFilter, Iters),
    SubOpts = maps:get(props, Iterator),
    ok = del_subscription(TopicFilter, ID),
    {ok, Session#{iterators := maps:remove(TopicFilter, Iters)}, SubOpts};
unsubscribe(
    _TopicFilter,
    _Session = #{}
) ->
    {error, ?RC_NO_SUBSCRIPTION_EXISTED}.

-spec get_subscription(topic(), session()) ->
    emqx_types:subopts() | undefined.
get_subscription(TopicFilter, #{iterators := Iters}) ->
    case maps:get(TopicFilter, Iters, undefined) of
        Iterator = #{} ->
            maps:get(props, Iterator);
        undefined ->
            undefined
    end.

%%--------------------------------------------------------------------
%% Client -> Broker: PUBLISH
%%--------------------------------------------------------------------

-spec publish(emqx_types:packet_id(), emqx_types:message(), session()) ->
    {ok, emqx_types:publish_result(), replies(), session()}
    | {error, emqx_types:reason_code()}.
publish(_PacketId, Msg, Session) ->
    %% TODO:
    Result = emqx_broker:publish(Msg),
    {ok, Result, [], Session}.

%%--------------------------------------------------------------------
%% Client -> Broker: PUBACK
%%--------------------------------------------------------------------

%% FIXME: parts of the commit offset function are mocked
-dialyzer({nowarn_function, puback/3}).

-spec puback(clientinfo(), emqx_types:packet_id(), session()) ->
    {ok, emqx_types:message(), replies(), session()}
    | {error, emqx_types:reason_code()}.
puback(_ClientInfo, PacketId, Session = #{id := Id, inflight := Inflight0}) ->
    case emqx_persistent_message_ds_replayer:commit_offset(Id, PacketId, Inflight0) of
        {true, Inflight} ->
            %% TODO
            Msg = #message{},
            {ok, Msg, [], Session#{inflight => Inflight}};
        {false, _} ->
            {error, ?RC_PACKET_IDENTIFIER_NOT_FOUND}
    end.

%%--------------------------------------------------------------------
%% Client -> Broker: PUBREC
%%--------------------------------------------------------------------

-spec pubrec(emqx_types:packet_id(), session()) ->
    {ok, emqx_types:message(), session()}
    | {error, emqx_types:reason_code()}.
pubrec(_PacketId, _Session = #{}) ->
    % TODO: stub
    {error, ?RC_PACKET_IDENTIFIER_NOT_FOUND}.

%%--------------------------------------------------------------------
%% Client -> Broker: PUBREL
%%--------------------------------------------------------------------

-spec pubrel(emqx_types:packet_id(), session()) ->
    {ok, session()} | {error, emqx_types:reason_code()}.
pubrel(_PacketId, Session = #{}) ->
    % TODO: stub
    {ok, Session}.

%%--------------------------------------------------------------------
%% Client -> Broker: PUBCOMP
%%--------------------------------------------------------------------

-spec pubcomp(clientinfo(), emqx_types:packet_id(), session()) ->
    {ok, emqx_types:message(), replies(), session()}
    | {error, emqx_types:reason_code()}.
pubcomp(_ClientInfo, _PacketId, _Session = #{}) ->
    % TODO: stub
    {error, ?RC_PACKET_IDENTIFIER_NOT_FOUND}.

%%--------------------------------------------------------------------

-spec deliver(clientinfo(), [emqx_types:deliver()], session()) ->
    {ok, replies(), session()}.
deliver(_ClientInfo, _Delivers, Session) ->
    %% TODO: QoS0 and system messages end up here.
    {ok, [], Session}.

-spec handle_timeout(clientinfo(), _Timeout, session()) ->
    {ok, replies(), session()} | {ok, replies(), timeout(), session()}.
handle_timeout(_ClientInfo, pull, Session = #{id := Id, inflight := Inflight0}) ->
    WindowSize = 1000,
    {Publishes, Inflight} = emqx_persistent_message_ds_replayer:poll(Id, Inflight0, WindowSize),
    %% TODO: make these values configurable:
    Timeout =
        case Publishes of
            [] ->
                100;
            [_ | _] ->
                0
        end,
    ensure_timer(pull, Timeout),
    {ok, Publishes, Session#{inflight => Inflight}};
handle_timeout(_ClientInfo, get_streams, Session = #{id := Id}) ->
    renew_streams(Id),
    ensure_timer(get_streams),
    {ok, [], Session}.

-spec replay(clientinfo(), [], session()) ->
    {ok, replies(), session()}.
replay(_ClientInfo, [], Session = #{}) ->
    {ok, [], Session}.

%%--------------------------------------------------------------------

-spec disconnect(session()) -> {shutdown, session()}.
disconnect(Session = #{}) ->
    {shutdown, Session}.

-spec terminate(Reason :: term(), session()) -> ok.
terminate(_Reason, _Session = #{}) ->
    % TODO: close iterators
    ok.

%%--------------------------------------------------------------------

-spec add_subscription(topic(), emqx_types:subopts(), id()) ->
    subscription().
add_subscription(TopicFilterBin, SubOpts, DSSessionID) ->
    %% N.B.: we chose to update the router before adding the subscription to the
    %% session/iterator table.  The reasoning for this is as follows:
    %%
    %% Messages matching this topic filter should start to be persisted as soon as
    %% possible to avoid missing messages.  If this is the first such persistent
    %% session subscription, it's important to do so early on.
    %%
    %% This could, in turn, lead to some inconsistency: if such a route gets
    %% created but the session/iterator data fails to be updated accordingly, we
    %% have a dangling route.  To remove such dangling routes, we may have a
    %% periodic GC process that removes routes that do not have a matching
    %% persistent subscription.  Also, route operations use dirty mnesia
    %% operations, which inherently have room for inconsistencies.
    %%
    %% In practice, we use the iterator reference table as a source of truth,
    %% since it is guarded by a transaction context: we consider a subscription
    %% operation to be successful if it ended up changing this table.  Both router
    %% and iterator information can be reconstructed from this table, if needed.
    ok = emqx_persistent_session_ds_router:do_add_route(TopicFilterBin, DSSessionID),
    TopicFilter = emqx_topic:words(TopicFilterBin),
    {ok, DSSubExt, IsNew} = session_add_subscription(
        DSSessionID, TopicFilter, SubOpts
    ),
    ?tp(persistent_session_ds_subscription_added, #{sub => DSSubExt, is_new => IsNew}),
    %% we'll list streams and open iterators when implementing message replay.
    DSSubExt.

-spec update_subscription(topic(), subscription(), emqx_types:subopts(), id()) ->
    subscription().
update_subscription(TopicFilterBin, DSSubExt, SubOpts, DSSessionID) ->
    TopicFilter = emqx_topic:words(TopicFilterBin),
    {ok, NDSSubExt, false} = session_add_subscription(
        DSSessionID, TopicFilter, SubOpts
    ),
    ok = ?tp(persistent_session_ds_iterator_updated, #{sub => DSSubExt}),
    NDSSubExt.

-spec del_subscription(topic(), id()) ->
    ok.
del_subscription(TopicFilterBin, DSSessionId) ->
    TopicFilter = emqx_topic:words(TopicFilterBin),
    ?tp_span(
        persistent_session_ds_subscription_delete,
        #{session_id => DSSessionId},
        ok = session_del_subscription(DSSessionId, TopicFilter)
    ),
    ?tp_span(
        persistent_session_ds_subscription_route_delete,
        #{session_id => DSSessionId},
        ok = emqx_persistent_session_ds_router:do_delete_route(TopicFilterBin, DSSessionId)
    ).

%%--------------------------------------------------------------------
%% Session tables operations
%%--------------------------------------------------------------------

create_tables() ->
    ok = emqx_ds:open_db(?PERSISTENT_MESSAGE_DB, #{
        backend => builtin,
        storage => {emqx_ds_storage_bitfield_lts, #{}}
    }),
    ok = mria:create_table(
        ?SESSION_TAB,
        [
            {rlog_shard, ?DS_MRIA_SHARD},
            {type, set},
            {storage, storage()},
            {record_name, session},
            {attributes, record_info(fields, session)}
        ]
    ),
    ok = mria:create_table(
        ?SESSION_SUBSCRIPTIONS_TAB,
        [
            {rlog_shard, ?DS_MRIA_SHARD},
            {type, ordered_set},
            {storage, storage()},
            {record_name, ds_sub},
            {attributes, record_info(fields, ds_sub)}
        ]
    ),
    ok = mria:create_table(
        ?SESSION_STREAM_TAB,
        [
            {rlog_shard, ?DS_MRIA_SHARD},
            {type, bag},
            {storage, storage()},
            {record_name, ds_stream},
            {attributes, record_info(fields, ds_stream)}
        ]
    ),
    ok = mria:create_table(
        ?SESSION_ITER_TAB,
        [
            {rlog_shard, ?DS_MRIA_SHARD},
            {type, set},
            {storage, storage()},
            {record_name, ds_iter},
            {attributes, record_info(fields, ds_iter)}
        ]
    ),
    ok = mria:wait_for_tables([
        ?SESSION_TAB, ?SESSION_SUBSCRIPTIONS_TAB, ?SESSION_STREAM_TAB, ?SESSION_ITER_TAB
    ]),
    ok.

-dialyzer({nowarn_function, storage/0}).
storage() ->
    %% FIXME: This is a temporary workaround to avoid crashes when starting on Windows
    case mria:rocksdb_backend_available() of
        true ->
            rocksdb_copies;
        _ ->
            disc_copies
    end.

%% @doc Called when a client connects. This function looks up a
%% session or returns `false` if previous one couldn't be found.
%%
%% Note: session API doesn't handle session takeovers, it's the job of
%% the broker.
-spec session_open(id()) ->
    {ok, session(), #{topic() => subscription()}} | false.
session_open(SessionId) ->
    transaction(fun() ->
        case mnesia:read(?SESSION_TAB, SessionId, write) of
            [Record = #session{}] ->
                Session = export_session(Record),
                DSSubs = session_read_subscriptions(SessionId),
                Subscriptions = export_subscriptions(DSSubs),
                {ok, Session, Subscriptions};
            [] ->
                false
        end
    end).

-spec session_ensure_new(id(), _Props :: map()) ->
    {ok, session(), #{topic() => subscription()}}.
session_ensure_new(SessionId, Props) ->
    transaction(fun() ->
        ok = session_drop_subscriptions(SessionId),
        Session = export_session(session_create(SessionId, Props)),
        {ok, Session, #{}}
    end).

session_create(SessionId, Props) ->
    Session = #session{
        id = SessionId,
        created_at = erlang:system_time(millisecond),
        expires_at = never,
        props = Props,
        inflight = emqx_persistent_message_ds_replayer:new()
    },
    ok = mnesia:write(?SESSION_TAB, Session, write),
    Session.

%% @doc Called when a client reconnects with `clean session=true' or
%% during session GC
-spec session_drop(id()) -> ok.
session_drop(DSSessionId) ->
    transaction(fun() ->
        ok = session_drop_subscriptions(DSSessionId),
        ok = session_drop_iterators(DSSessionId),
        ok = session_drop_streams(DSSessionId),
        ok = mnesia:delete(?SESSION_TAB, DSSessionId, write)
    end).

-spec session_drop_subscriptions(id()) -> ok.
session_drop_subscriptions(DSSessionId) ->
    Subscriptions = session_read_subscriptions(DSSessionId),
    lists:foreach(
        fun(#ds_sub{id = DSSubId} = DSSub) ->
            TopicFilter = subscription_id_to_topic_filter(DSSubId),
            TopicFilterBin = emqx_topic:join(TopicFilter),
            ok = emqx_persistent_session_ds_router:do_delete_route(TopicFilterBin, DSSessionId),
            ok = session_del_subscription(DSSub)
        end,
        Subscriptions
    ).

%% @doc Called when a client subscribes to a topic. Idempotent.
-spec session_add_subscription(id(), topic_filter(), _Props :: map()) ->
    {ok, subscription(), _IsNew :: boolean()}.
session_add_subscription(DSSessionId, TopicFilter, Props) ->
    DSSubId = {DSSessionId, TopicFilter},
    transaction(fun() ->
        case mnesia:read(?SESSION_SUBSCRIPTIONS_TAB, DSSubId, write) of
            [] ->
                DSSub = session_insert_subscription(DSSessionId, TopicFilter, Props),
                DSSubExt = export_subscription(DSSub),
                ?tp(
                    ds_session_subscription_added,
                    #{sub => DSSubExt, session_id => DSSessionId}
                ),
                {ok, DSSubExt, _IsNew = true};
            [#ds_sub{} = DSSub] ->
                NDSSub = session_update_subscription(DSSub, Props),
                NDSSubExt = export_subscription(NDSSub),
                ?tp(
                    ds_session_subscription_present,
                    #{sub => NDSSubExt, session_id => DSSessionId}
                ),
                {ok, NDSSubExt, _IsNew = false}
        end
    end).

-spec session_insert_subscription(id(), topic_filter(), map()) -> ds_sub().
session_insert_subscription(DSSessionId, TopicFilter, Props) ->
    {DSSubId, StartMS} = new_subscription_id(DSSessionId, TopicFilter),
    DSSub = #ds_sub{
        id = DSSubId,
        start_time = StartMS,
        props = Props,
        extra = #{}
    },
    ok = mnesia:write(?SESSION_SUBSCRIPTIONS_TAB, DSSub, write),
    DSSub.

-spec session_update_subscription(ds_sub(), map()) -> ds_sub().
session_update_subscription(DSSub, Props) ->
    NDSSub = DSSub#ds_sub{props = Props},
    ok = mnesia:write(?SESSION_SUBSCRIPTIONS_TAB, NDSSub, write),
    NDSSub.

session_del_subscription(DSSessionId, TopicFilter) ->
    DSSubId = {DSSessionId, TopicFilter},
    transaction(fun() ->
        mnesia:delete(?SESSION_SUBSCRIPTIONS_TAB, DSSubId, write)
    end).

session_del_subscription(#ds_sub{id = DSSubId}) ->
    mnesia:delete(?SESSION_SUBSCRIPTIONS_TAB, DSSubId, write).

session_read_subscriptions(DSSessionId) ->
    MS = ets:fun2ms(
        fun(Sub = #ds_sub{id = {Sess, _}}) when Sess =:= DSSessionId ->
            Sub
        end
    ),
    mnesia:select(?SESSION_SUBSCRIPTIONS_TAB, MS, read).

-spec new_subscription_id(id(), topic_filter()) -> {subscription_id(), integer()}.
new_subscription_id(DSSessionId, TopicFilter) ->
    %% Note: here we use _milliseconds_ to match with the timestamp
    %% field of `#message' record.
    NowMS = erlang:system_time(millisecond),
    DSSubId = {DSSessionId, TopicFilter},
    {DSSubId, NowMS}.

-spec subscription_id_to_topic_filter(subscription_id()) -> topic_filter().
subscription_id_to_topic_filter({_DSSessionId, TopicFilter}) ->
    TopicFilter.

%%--------------------------------------------------------------------
%% RPC targets (v1)
%%--------------------------------------------------------------------

%% RPC target.
-spec do_open_iterator(emqx_types:words(), emqx_ds:time(), emqx_ds:iterator_id()) ->
    {ok, emqx_ds_storage_layer:iterator()} | {error, _Reason}.
do_open_iterator(_TopicFilter, _StartMS, _IteratorID) ->
    {error, not_implemented}.

%% RPC target.
-spec do_ensure_iterator_closed(emqx_ds:iterator_id()) -> ok.
do_ensure_iterator_closed(_IteratorID) ->
    ok.

%% RPC target.
-spec do_ensure_all_iterators_closed(id()) -> ok.
do_ensure_all_iterators_closed(_DSSessionID) ->
    ok.

%%--------------------------------------------------------------------
%% Reading batches
%%--------------------------------------------------------------------

-spec renew_streams(id()) -> ok.
renew_streams(DSSessionId) ->
    Subscriptions = ro_transaction(fun() -> session_read_subscriptions(DSSessionId) end),
    ExistingStreams = ro_transaction(fun() -> mnesia:read(?SESSION_STREAM_TAB, DSSessionId) end),
    lists:foreach(
        fun(#ds_sub{id = {_, TopicFilter}, start_time = StartTime}) ->
            renew_streams(DSSessionId, ExistingStreams, TopicFilter, StartTime)
        end,
        Subscriptions
    ).

-spec renew_streams(id(), [ds_stream()], emqx_ds:topic_filter(), emqx_ds:time()) -> ok.
renew_streams(DSSessionId, ExistingStreams, TopicFilter, StartTime) ->
    AllStreams = emqx_ds:get_streams(?PERSISTENT_MESSAGE_DB, TopicFilter, StartTime),
    transaction(
        fun() ->
            lists:foreach(
                fun({Rank, Stream}) ->
                    Rec = #ds_stream{
                        session = DSSessionId,
                        topic_filter = TopicFilter,
                        stream = Stream,
                        rank = Rank
                    },
                    case lists:member(Rec, ExistingStreams) of
                        true ->
                            ok;
                        false ->
                            mnesia:write(?SESSION_STREAM_TAB, Rec, write),
                            {ok, Iterator} = emqx_ds:make_iterator(
                                ?PERSISTENT_MESSAGE_DB, Stream, TopicFilter, StartTime
                            ),
                            %% Workaround: we convert `Stream' to a binary before
                            %% attempting to store it in mnesia(rocksdb) because of a bug
                            %% in `mnesia_rocksdb' when trying to do
                            %% `mnesia:dirty_all_keys' later.
                            StreamBin = term_to_binary(Stream),
                            IterRec = #ds_iter{id = {DSSessionId, StreamBin}, iter = Iterator},
                            mnesia:write(?SESSION_ITER_TAB, IterRec, write)
                    end
                end,
                AllStreams
            )
        end
    ).

%% must be called inside a transaction
-spec session_drop_streams(id()) -> ok.
session_drop_streams(DSSessionId) ->
    MS = ets:fun2ms(
        fun(#ds_stream{session = DSSessionId0}) when DSSessionId0 =:= DSSessionId ->
            DSSessionId0
        end
    ),
    StreamIDs = mnesia:select(?SESSION_STREAM_TAB, MS, write),
    lists:foreach(fun(Key) -> mnesia:delete(?SESSION_STREAM_TAB, Key, write) end, StreamIDs).

%% must be called inside a transaction
-spec session_drop_iterators(id()) -> ok.
session_drop_iterators(DSSessionId) ->
    MS = ets:fun2ms(
        fun(#ds_iter{id = {DSSessionId0, StreamBin}}) when DSSessionId0 =:= DSSessionId ->
            StreamBin
        end
    ),
    StreamBins = mnesia:select(?SESSION_ITER_TAB, MS, write),
    lists:foreach(
        fun(StreamBin) ->
            mnesia:delete(?SESSION_ITER_TAB, {DSSessionId, StreamBin}, write)
        end,
        StreamBins
    ).

%%--------------------------------------------------------------------------------

transaction(Fun) ->
    {atomic, Res} = mria:transaction(?DS_MRIA_SHARD, Fun),
    Res.

ro_transaction(Fun) ->
    {atomic, Res} = mria:ro_transaction(?DS_MRIA_SHARD, Fun),
    Res.

%%--------------------------------------------------------------------------------

export_subscriptions(DSSubs) ->
    lists:foldl(
        fun(DSSub = #ds_sub{id = {_DSSessionId, TopicFilter}}, Acc) ->
            Acc#{TopicFilter => export_subscription(DSSub)}
        end,
        #{},
        DSSubs
    ).

export_session(#session{} = Record) ->
    export_record(Record, #session.id, [id, created_at, expires_at, inflight, props], #{}).

export_subscription(#ds_sub{} = Record) ->
    export_record(Record, #ds_sub.start_time, [start_time, props, extra], #{}).

export_record(Record, I, [Field | Rest], Acc) ->
    export_record(Record, I + 1, Rest, Acc#{Field => element(I, Record)});
export_record(_, _, [], Acc) ->
    Acc.

%% TODO: find a more reliable way to perform actions that have side
%% effects. Add `CBM:init' callback to the session behavior?
ensure_timers() ->
    ensure_timer(pull),
    ensure_timer(get_streams).

-spec ensure_timer(pull | get_streams) -> ok.
ensure_timer(Type) ->
    ensure_timer(Type, 100).

-spec ensure_timer(pull | get_streams, non_neg_integer()) -> ok.
ensure_timer(Type, Timeout) ->
    _ = emqx_utils:start_timer(Timeout, {emqx_session, Type}),
    ok.

-ifdef(TEST).
list_all_sessions() ->
    DSSessionIds = mnesia:dirty_all_keys(?SESSION_TAB),
    Sessions = lists:map(
        fun(SessionID) ->
            {ok, Session, Subscriptions} = session_open(SessionID),
            {SessionID, #{session => Session, subscriptions => Subscriptions}}
        end,
        DSSessionIds
    ),
    maps:from_list(Sessions).

list_all_subscriptions() ->
    DSSubIds = mnesia:dirty_all_keys(?SESSION_SUBSCRIPTIONS_TAB),
    Subscriptions = lists:map(
        fun(DSSubId) ->
            [DSSub] = mnesia:dirty_read(?SESSION_SUBSCRIPTIONS_TAB, DSSubId),
            {DSSubId, export_subscription(DSSub)}
        end,
        DSSubIds
    ),
    maps:from_list(Subscriptions).

list_all_streams() ->
    DSStreamIds = mnesia:dirty_all_keys(?SESSION_STREAM_TAB),
    DSStreams = lists:map(
        fun(DSStreamId) ->
            Records = mnesia:dirty_read(?SESSION_STREAM_TAB, DSStreamId),
            ExtDSStreams =
                lists:map(
                    fun(Record) ->
                        export_record(
                            Record,
                            #ds_stream.session,
                            [session, topic_filter, stream, rank],
                            #{}
                        )
                    end,
                    Records
                ),
            {DSStreamId, ExtDSStreams}
        end,
        DSStreamIds
    ),
    maps:from_list(DSStreams).

list_all_iterators() ->
    DSIterIds = mnesia:dirty_all_keys(?SESSION_ITER_TAB),
    DSIters = lists:map(
        fun(DSIterId) ->
            [Record] = mnesia:dirty_read(?SESSION_ITER_TAB, DSIterId),
            {DSIterId, export_record(Record, #ds_iter.id, [id, iter], #{})}
        end,
        DSIterIds
    ),
    maps:from_list(DSIters).

%% ifdef(TEST)
-endif.
