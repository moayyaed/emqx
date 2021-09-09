%%--------------------------------------------------------------------
%% Copyright (c) 2018-2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% @doc The session router worker is responsible for buffering
%% messages for a persistent session while it is initializing.  If a
%% connection process exists for a persistent session, this process is
%% used for bridging the gap while the new connection process takes
%% over the persistent session, but if there is no such process this
%% worker takes it place.
%%
%% The workers are started on all nodes, and buffers all messages that
%% are persisted to the session message table. In the final stage of
%% the initialization, the messages are delivered and the worker is
%% terminated.


-module(emqx_session_router_worker).

-behaviour(gen_server).

%% API
-export([ pendings/1
        , resume_end/2
        , start_link/2
        ]).

%% gen_server callbacks
-export([ init/1
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , terminate/2
        ]).

-include("logger.hrl").

-define(SERVER, ?MODULE).

-record(state, { remote_pid :: pid()
               , session_id :: binary()
               , session_tab :: ets:table()
               , messages :: ets:table()
               , buffering :: boolean()
               }).

%%%===================================================================
%%% API
%%%===================================================================

start_link(SessionTab, #{} = Opts) ->
    gen_server:start_link(?MODULE, Opts#{ session_tab => SessionTab}, []).

pendings(Pid) ->
    gen_server:call(Pid, pendings).

resume_end(RemotePid, Pid) ->
    case gen_server:call(Pid, {resume_end, RemotePid}) of
        {ok, EtsHandle} ->
            {ok, ets:tab2list(EtsHandle)};
        {error, _} = Err ->
            Err
    end.

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init(#{ remote_pid  := RemotePid
      , session_id  := SessionID
      , session_tab := SessionTab}) ->
    process_flag(trap_exit, true),
    link(RemotePid),
    {ok, #state{ remote_pid = RemotePid
               , session_id = SessionID
               , session_tab = SessionTab
               , messages = ets:new(?MODULE, [protected, ordered_set])
               , buffering = true
               }}.

handle_call(pendings, _From, State) ->
    %% Debug API
    {reply, {State#state.messages, State#state.remote_pid}, State};
handle_call(resume_end, _From, State) ->
    {reply, {State#state.messages, State#state.remote_pid}, State};
handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

handle_cast(_Request, State) ->
    {noreply, State}.

handle_info({deliver, Msg}, State) when State#state.buffering ->
    ets:insert(State#state.messages, Msg),
    {noreply, State};
handle_info({'EXIT', RemotePid, _}, #state{remote_pid = RemotePid} = State) ->
    ?SLOG(warning, #{msg => "Remote pid died. Exiting."}),
    {stop, normal, State};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(shutdown, _State) ->
    ?SLOG(debug, #{msg => "Shutdown on request"}),
    ok;
terminate(_, _State) ->
    ok.

%%%===================================================================
%%% Internal functions
%%%===================================================================
