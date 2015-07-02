%% -------------------------------------------------------------------
%%
%% riak_kv_backend: Riak backend behaviour
%%
%% Copyright (c) 2007-2013 Basho Technologies, Inc.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------
-module(dvv_fix).

-export([fix_buckets/0]).
 
fix_buckets() ->
  fix_dvv_lww_bucket_types(),
  fix_dvv_lww_buckets(),
  warn_default_bucket_type(),
  ok.
 
fix_dvv_lww_buckets() ->
  {ok, Ring} = riak_core_ring_manager:get_my_ring(),
  lists:foreach(fun(Props) ->
                    case bucket_affected(Props) of
                      true ->
                        Name = proplists:get_value(name, Props),
                        print_fixing(Name),
                        riak_core_bucket:set_bucket(Name, [{dvv_enabled, false}]);
                      _ ->
                        ok
                    end
                end,
                riak_core_bucket:get_buckets(Ring)).
 
fix_dvv_lww_bucket_types() ->
    It = riak_core_bucket_type:iterator(),
    fix_dvv_lww_bucket_types(It).
 
fix_dvv_lww_bucket_types(It) ->
    case riak_core_bucket_type:itr_done(It) of
        true ->
            riak_core_bucket_type:itr_close(It);
        false ->
            {Type, Props} = riak_core_bucket_type:itr_value(It),
            case bucket_affected(Props) of
              true ->
                print_fixing(Type),
                riak_core_bucket_type:update(Type, [{dvv_enabled, false}]);
              _ ->
                ok
            end,
            fix_dvv_lww_bucket_types(riak_core_bucket_type:itr_next(It))
    end.
 
warn_default_bucket_type() ->
  case bucket_affected(riak_core_bucket_props:defaults()) of
    true ->
      io:format("%%~n"),
      io:format("%% WARNING: Default bucket properties have been set with last_write_wins and dvv_enabled set to true!~n"),
      io:format("%% Fix Riak configuration so that dvv_enabled is set to false~n"),
      io:format("%%~n");
    _ -> ok
  end.
 
bucket_affected(Props) ->
  {true, true} == {proplists:get_value(last_write_wins, Props, false), proplists:get_value(dvv_enabled, Props, false)}.
 
print_fixing(Name) ->
  io:format("!! Fixing ~p: - resetting dvv_enabled=false~n", [Name]).
