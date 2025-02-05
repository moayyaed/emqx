%%--------------------------------------------------------------------
%% Copyright (c) 2017-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-ifndef(EMQX_HRL).
-define(EMQX_HRL, true).

%% Shard
%%--------------------------------------------------------------------
-define(COMMON_SHARD, emqx_common_shard).
-define(SHARED_SUB_SHARD, emqx_shared_sub_shard).
-define(CM_SHARD, emqx_cm_shard).
-define(ROUTE_SHARD, route_shard).
-define(PS_ROUTER_SHARD, persistent_session_router_shard).

%% Banner
%%--------------------------------------------------------------------

-define(PROTOCOL_VERSION, "MQTT/5.0").

-define(ERTS_MINIMUM_REQUIRED, "10.0").

%%--------------------------------------------------------------------
%% Topics' prefix: $SYS | $queue | $share
%%--------------------------------------------------------------------

%% System topic
-define(SYSTOP, <<"$SYS/">>).

%% Queue topic
-define(QUEUE, <<"$queue/">>).

%%--------------------------------------------------------------------
%% alarms
%%--------------------------------------------------------------------
-define(ACTIVATED_ALARM, emqx_activated_alarm).
-define(DEACTIVATED_ALARM, emqx_deactivated_alarm).
-define(TRIE, emqx_trie).

%%--------------------------------------------------------------------
%% Message and Delivery
%%--------------------------------------------------------------------

-record(subscription, {topic, subid, subopts}).

-include_lib("emqx_utils/include/emqx_message.hrl").

-record(delivery, {
    %% Sender of the delivery
    sender :: pid(),
    %% The message delivered
    message :: #message{}
}).

%%--------------------------------------------------------------------
%% Route
%%--------------------------------------------------------------------

-record(route, {
    topic :: binary(),
    dest :: node() | {binary(), node()} | emqx_session:session_id()
}).

%%--------------------------------------------------------------------
%% Command
%%--------------------------------------------------------------------

-record(command, {
    name :: atom(),
    action :: atom(),
    args = [] :: list(),
    opts = [] :: list(),
    usage :: string(),
    descr :: string()
}).

%%--------------------------------------------------------------------
%% Banned
%%--------------------------------------------------------------------

-record(banned, {
    who ::
        {clientid, binary()}
        | {peerhost, inet:ip_address()}
        | {username, binary()},
    by :: binary(),
    reason :: binary(),
    at :: integer(),
    until :: integer()
}).

-endif.
