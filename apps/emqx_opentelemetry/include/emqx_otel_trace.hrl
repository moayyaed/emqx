%%--------------------------------------------------------------------
%% Copyright (c) 2023-2024 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-ifndef(EMQX_OTEL_TRACE_HRL).
-define(EMQX_OTEL_TRACE_HRL, true).

-define(CLIENT_CONNECT_SPAN_NAME, <<"client.connect">>).
-define(CLIENT_DISCONNECT_SPAN_NAME, <<"client.disconnect">>).
-define(CLIENT_SUBSCRIBE_SPAN_NAME, <<"client.subscribe">>).
-define(CLIENT_UNSUBSCRIBE_SPAN_NAME, <<"client.unsubscribe">>).

-endif.
