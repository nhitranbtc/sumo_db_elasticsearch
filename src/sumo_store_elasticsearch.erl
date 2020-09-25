%%% @hidden
%%% @doc ElasticSearch store implementation.
%%%
%%% Copyright 2012 Inaka &lt;hello@inaka.net&gt;
%%%
%%% Licensed under the Apache License, Version 2.0 (the "License");
%%% you may not use this file except in compliance with the License.
%%% You may obtain a copy of the License at
%%%
%%%     http://www.apache.org/licenses/LICENSE-2.0
%%%
%%% Unless required by applicable law or agreed to in writing, software
%%% distributed under the License is distributed on an "AS IS" BASIS,
%%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%%% See the License for the specific language governing permissions and
%%% limitations under the License.
%%% @end
%%% @copyright Inaka <hello@inaka.net>
%%%
-module(sumo_store_elasticsearch).
-author("Juan Facorro <juan@inaka.net>").
-license("Apache License 2.0").

-behavior(sumo_store).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Exports.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Public API.
-export([
				 init/1,
				 create_schema/2,
				 delete_schema/1,
				 persist/2,
				 find_by/3,
				 find_by/5,
				 find_by/6,
				 find_count_by/6,
				 find_all/2,
				 find_all/5,
				 delete_by/3,
				 delete_all/2
				]).

-export([
				 sleep/1,
				 wakeup/1
				]).

-define(NESTED_SEP_Q, <<"#">>).
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Types.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-type state() ::
#{index => binary(),
	pool_name => atom()}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% External API.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec init(term()) -> {ok, term()}.
init(Options) ->
	%% ElasticSearch client uses poolboy to handle its own pool of workers
	%% so no pool is required.
	Backend = proplists:get_value(storage_backend, Options),
	% Index = case sumo_backend_elasticsearch:get_index(Backend) of
	%             Idx when is_list(Idx) -> list_to_binary(Idx);
	%             Idx when is_binary(Idx) -> Idx;
	%             _ -> throw(invalid_index)
	%         end,
	Index =
	case proplists:get_value(index, Options, <<>>) of
		Idx when is_list(Idx) -> list_to_binary(Idx);
		Idx when is_binary(Idx) -> Idx;
		_ -> throw(invalid_index)
	end,
	%lager:debug("store init Backend: ~p, Index: ~p~n",[Backend, Index]),
	PoolName = sumo_backend_elasticsearch:get_pool_name(Backend),

	{ok, #{index => Index, pool_name => PoolName}}.

-spec persist(sumo_internal:doc(), state()) ->
	sumo_store:result(sumo_internal:doc(), state()).
persist(Doc, #{index := Index, pool_name := PoolName} = State) ->
	DocName = sumo_internal:doc_name(Doc),
	Type = atom_to_binary(DocName, utf8),

	IdField = sumo_internal:id_field_name(DocName),
	io:format("IdField ~p ~n", [IdField]),
	Id =  sumo_internal:get_field(IdField, Doc),
	io:format("Id ~p ~n", [Id]),

	NewDoc = sleep(Doc),
	Fields = sumo_internal:doc_fields(NewDoc),
	Params = [{refresh, true}],
	{ok, _Json} = tirerl:insert_doc(PoolName, Index, Type, Id, Fields, Params),
	%{UpdateId, Update} =  {Id, #{doc => Fields}},
	% {UpdateId, Update} =
	%     case Id of
	%         undefined ->
	%             {ok, Json} =
	%                 tirerl:insert_doc(PoolName, Index, Type, Id, Fields),
	%             %% Get the Id that was assigned by elasticsearch.
	%             GenId = maps:get(<<"_id">>, Json),
	%             {GenId, #{doc => maps:from_list([{IdField, GenId}])}};
	%         Id ->
	%             {Id, #{doc => Fields}}
	%     end,
	%{ok, _ } = tirerl:update_doc(PoolName, Index, Type, UpdateId, Update),
	Doc1 = sumo_internal:set_field(IdField, Id, Doc),

	{ok, Doc1, State}.

-spec delete_by(sumo:schema_name(), sumo:conditions(), state()) ->
	sumo_store:result(sumo_store:affected_rows(), state()).
delete_by(DocName,
					Conditions,
					#{index := Index, pool_name := PoolName} = State) ->
	Query = build_delete_query(Conditions),
	Type = atom_to_binary(DocName, utf8),
	Count = count(PoolName, Index, Type, Query, []),
	ok = delete_by_query(PoolName, Index, Type, Query, []),
	{ok, Count, State}.

-spec delete_all(sumo:schema_name(), state()) ->
	sumo_store:result(sumo_store:affected_rows(), state()).
delete_all(DocName, #{index := Index, pool_name := PoolName} = State) ->
	Type = atom_to_binary(DocName, utf8),
	MatchAll = #{query => #{match_all => #{}}},

	Count = count(PoolName, Index, Type, MatchAll, []),
	ok = delete_by_query(PoolName, Index, Type, MatchAll, []),

	{ok, Count, State}.

-spec find_by(sumo:schema_name(),
							sumo:conditions(),
							non_neg_integer(),
							non_neg_integer(),
							state()) ->
	sumo_store:result([sumo_internal:doc()], state()).
find_by(DocName, Conditions, Limit, Offset, State) ->
	find_by(DocName, Conditions,[], Limit, Offset, State).

-spec find_count_by(sumo:schema_name(),
										sumo:conditions(),
										sumo:sort(),
										non_neg_integer(),
										non_neg_integer(),
										state()) ->
	sumo_store:result([sumo_internal:doc()], state()).

find_count_by(DocName, Conditions, SortFields, Limit, Offset, #{index := Index, pool_name := PoolName} = State) ->
	Type = atom_to_binary(DocName, utf8),
	Query = build_query(DocName, Conditions, SortFields, Limit, Offset),
	SearchResults = tirerl:search(PoolName, Index, Type, Query),
	case SearchResults of
		{ok, #{<<"aggregations">> := AggResult}} -> {ok, {agg, AggResult}, State};
		{ok, #{<<"hits">> := #{<<"hits">> := Results, <<"total">> := #{<<"value">> := Total} }}} ->
			Fun = fun(Item) ->
								wakeup(map_to_doc(DocName, Item)) end,
			Docs = lists:map(Fun, Results),
			{ok, {Total, Docs}, State};
		_ -> {ok, {0,[]}, State}
	end.
-spec find_by(sumo:schema_name(),
							sumo:conditions(),
							sumo:sort(),
							non_neg_integer(),
							non_neg_integer(),
							state()) ->
	sumo_store:result([sumo_internal:doc()], state()).

find_by(DocName, Conditions, SortFields, Limit, Offset, #{index := Index, pool_name := PoolName} = State) ->
	Type = atom_to_binary(DocName, utf8),
	Query = build_query(DocName, Conditions, SortFields, Limit, Offset),
	SearchResults = tirerl:search(PoolName, Index, Type, Query),
	case SearchResults of
		{ok, #{<<"aggregations">> := AggResult}} -> {ok, {agg, AggResult}, State};
		{ok, #{<<"hits">> := #{<<"hits">> := Results}}} ->
			Fun = fun(Item) ->
								wakeup(map_to_doc(DocName, Item)) end,
			Docs = lists:map(Fun, Results),
			{ok, Docs, State};
		_ -> {ok, [], State}
	end.


-spec find_by(sumo:schema_name(), sumo:conditions(), state()) ->
	sumo_store:result([sumo_internal:doc()], state()).
find_by(DocName, Conditions, State) ->
	find_by(DocName, Conditions, 200, 0, State).

-spec find_all(sumo:schema_name(), state()) ->
	sumo_store:result([sumo_internal:doc()], state()).
find_all(DocName, State) ->
	find_by(DocName, [], State).

-spec find_all(sumo:schema_name(),
							 term(),
							 non_neg_integer(),
							 non_neg_integer(),
							 state()) ->
	sumo_store:result([sumo_internal:doc()], state()).
find_all(DocName, _OrderField, Limit, Offset, State) ->
	find_by(DocName, [], Limit, Offset, State).

-spec create_schema(sumo:schema(), state()) -> sumo_store:result(state()).
create_schema(Schema, #{index := Index, pool_name := PoolName} = State) ->
	SchemaName = sumo_internal:schema_name(Schema),
	Type = atom_to_binary(SchemaName, utf8),
	Fields = sumo_internal:schema_fields(Schema),
	Mapping = build_mapping(SchemaName, Fields),
	_ = case tirerl:is_index(PoolName, Index) of
				false -> tirerl:create_index(PoolName, Index);
				_ -> ok
			end,


	{ok, _} = tirerl:put_mapping(PoolName, Index, Type, Mapping),

	{ok, State}.

-spec delete_schema(state()) -> sumo_store:result(state()).
delete_schema(#{index := Index, pool_name := PoolName} = State) ->

	tirerl:delete_index(PoolName, Index),

	{ok, State}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Internal Functions.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

map_to_doc(DocName, Item) ->
	Values = maps:get(<<"_source">>, Item),
	IdField = sumo_internal:id_field_name(DocName),
	Fun = fun (Key, Doc) ->
						FieldName = binary_to_atom(Key, utf8),
						Value = maps:get(Key, Values),
						sumo_internal:set_field(FieldName, Value, Doc)
				end,
	Keys = maps:keys(Values),
	Doc = lists:foldl(Fun, sumo_internal:new_doc(DocName), Keys),
	sumo_internal:set_field(IdField, maps:get(<<"_id">>, Item), Doc).

% build_query(Conditions) ->
%     build_query(Conditions, [], 0, 0).

% build_query(Conditions, Limit, Offset) ->
%   build_query(Conditions,[], Limit, Offset).

build_query(DocName, Conditions, SortConditions, Limit, Offset) ->
	FilterQuery = build_query_conditions(Conditions),
	SortQuery = build_sort_conditions(DocName, SortConditions),
	Query = FilterQuery#{
						sort => SortQuery
					 },
	case Limit of
		0 -> Query#{from => Offset,
								size => 9000};
		_ -> Query#{from => Offset,
								size => Limit}
	end.

build_delete_query(Conditions) ->
	build_delete_query(Conditions, 0, 0).


build_delete_query(Conditions, Limit, Offset) ->
	Query = build_query_conditions(Conditions),

	case Limit of
		0 -> Query;
		_ -> Query#{from => Offset,
								size => Limit}
	end.

build_sort_conditions(_, []) ->
	[];

build_sort_conditions(DocName, Conditions) when is_list(Conditions) ->
	{SortConditions, _} =
	lists:mapfoldl(fun build_sort_conditions/2, DocName, Conditions),
	SortConditions;

build_sort_conditions({SortFieldRaw, SortValue}, DocName) ->
	SortFieldAtom = zt_util:to_atom(SortFieldRaw),
	SortTerm =
	case sumo_internal:field_type(DocName, SortFieldAtom) of
		string ->
			SortFieldBin = zt_util:to_bin(SortFieldRaw),
			SortField = zt_util:to_atom(<<SortFieldBin/binary,".keyword">>),
			#{
																													SortField => SortValue
																												 };
		_ ->
			#{
			SortFieldRaw => SortValue
		 }
	end,
	{SortTerm, DocName}.


build_agg_conditions(Conditions) when is_list(Conditions) ->
	Aggs = lists:filtermap(fun(Cond)->
														 case build_agg_conditions(Cond) of
															 ignore -> false;
															 FormatedCond -> {true, FormatedCond}
														 end
												 end, Conditions),
	case Aggs of
		[AggMap] -> AggMap;
		_ -> ignore
	end;

build_agg_conditions({agg, Body}) ->
	Body;

build_agg_conditions(_) ->
	ignore.

build_query_conditions([]) ->
	#{};

build_query_conditions(Conditions) when is_list(Conditions) ->
	QueryConditions = lists:filtermap(fun(Cond)->
																				case build_query_conditions(Cond) of
																					ignore -> false;
																					FormatedCond -> {true, FormatedCond}
																				end
																		end, Conditions),
	QueryMap = #{
		query => #{bool => #{must => QueryConditions}}
	 },
	case build_agg_conditions(Conditions) of
		ignore -> QueryMap;
		AggMap -> maps:put(aggregations, AggMap, QueryMap)
	end;

build_query_conditions({Key, Value}) when is_list(Value) ->
	#{match => maps:from_list([{Key, list_to_binary(Value)}])};

%TODO: Currently only handle with nested field => handle with object field


build_query_conditions({agg, _Body}) ->
	ignore;

build_query_conditions({Key, Value}) ->
	%lager:debug("build_query_conditions Key: ~p, Value: ~p~n",[Key, Value]),
	KeyBin = zt_util:to_bin(Key),
	case binary:split(KeyBin, ?NESTED_SEP_Q) of
		[KeyBin] ->
			#{match => maps:from_list([{Key, Value}])};
		[Key1 | _] ->
			NestedKey = binary:replace(KeyBin, ?NESTED_SEP_Q , <<".">>,[global]),
			#{
																																		nested => #{
																																			path => Key1,
																																			query => #{
																																				match => #{
																																					NestedKey => Value
																																				 }
																																			 }
																																		 }
																																	 }
	end;

build_query_conditions({Key, 'in', Values} = _Expr) ->
	KeyBin = zt_util:to_bin(Key),
	case binary:split(KeyBin, ?NESTED_SEP_Q) of
		[KeyBin] ->
			#{
			 terms => #{
				 Key => Values
				}
			};
		[Key1 | _] ->
			NestedKey = binary:replace(KeyBin, ?NESTED_SEP_Q , <<".">>,[global]),
			#{
																																		nested => #{
																																			path => Key1,
																																			query => #{
																																				terms => #{
																																					NestedKey => Values
																																				 }
																																			 }
																																		 }
																																	 }
	end;

build_query_conditions({Key, 'not in', Values} = _Expr) ->

	KeyBin = zt_util:to_bin(Key),
	case binary:split(KeyBin, ?NESTED_SEP_Q) of
		[KeyBin] ->
			#{
			 bool => #{
				 must_not => [
											#{
					 terms => #{
						 Key => Values
						}
					}
										 ]
				}
			};
		[Key1 | _] ->
			NestedKey = binary:replace(KeyBin, ?NESTED_SEP_Q , <<".">>,[global]),
			#{
																																		nested => #{
																																			path => Key1,
																																			query => #{
																																				bool => #{
																																					must_not => [
																																											 #{
																																						terms => #{
																																							NestedKey => Values
																																						 }
																																					 }
																																											]
																																				 }
																																			 }
																																		 }
																																	 }
	end;




build_query_conditions({Key, Op , Value}) when is_list(Value) ->
	build_query_conditions({Key, Op , list_to_binary(Value)});


build_query_conditions({Key, 'like', Value}) ->
	#{
																			 wildcard => #{
																				 Key => #{
																					 value => Value,
																					 boost => 1.0,
																					 rewrite => <<"constant_score">>
																					}
																				}
																			};



build_query_conditions({{field,Field1}, Op, {field, Field2}}) ->
	Field1Bin = zt_util:to_bin(Field1),
	Field2Bin = zt_util:to_bin(Field2),
	OpBin = zt_util:to_bin(Op),

	#{
													 script => #{
														 script => #{
															 source => <<"doc['",Field1Bin/binary,"'].value ",OpBin/binary," doc['",Field2Bin/binary,"'].value">>,
															 lang => <<"painless">>
															}
														}
													};

build_query_conditions({{field,Field1}, Op, Field2}) ->
	build_query_conditions({{field,Field1}, Op, {field, Field2}});

build_query_conditions({Field1, Op, {field, Field2}}) ->
	build_query_conditions({{field,Field1}, Op, {field, Field2}});

build_query_conditions({Key, '>', Value}) ->
	#{
																		range => #{
																			Key => #{
																				gt => Value
																			 }
																		 }
																	 };

build_query_conditions({Key, '>=', Value}) ->
	#{
																		 range => #{
																			 Key => #{
																				 gte => Value
																				}
																			}
																		};

build_query_conditions({Key, '<', Value}) ->
	#{
																		range => #{
																			Key => #{
																				lt => Value
																			 }
																		 }
																	 };

build_query_conditions({Key, '=<', Value}) ->
	#{
																		 range => #{
																			 Key => #{
																				 lte => Value
																				}
																			}
																		};

% CurLocation Distance Key



build_query_conditions({Key, 'distance', {CurLocation, DistanceVal}}) ->
	build_query_conditions({Key, 'distance', {CurLocation, DistanceVal, <<"km">>}});

build_query_conditions({Key, 'distance', {CurLocation, DistanceVal, DistanceUnit}}) ->
	CurLocationBin = zt_util:to_bin(CurLocation),
	DistanceValBin = zt_util:to_bin(DistanceVal),
	DistanceUnitBin = zt_util:to_bin(DistanceUnit),
	Distance = <<DistanceValBin/binary,DistanceUnitBin/binary>>,
	#{
																											 geo_distance => #{
																												 distance => Distance,
																												 Key => CurLocationBin
																												}
																											};

build_query_conditions(Expr) ->
	throw({unimplemented_expression, Expr}).

build_mapping(SchemaName, Fields) ->
	Fun =
	fun
		(Field, Acc) ->
			Name = sumo_internal:field_name(Field),
			TempType = sumo_internal:field_type(Field),
			FullFieldType =
			if TempType == string ->
					 FieldType = normalize_type(TempType),
					 #{type => FieldType,
						 fields => #{
							 keyword => #{
								 type => keyword
								}
							}
						};
				 TempType == object ->
					 Attrs = sumo_internal:field_attrs(Field),
					 case Attrs of
						 [] -> #{type => TempType, enabled => false};
						 [Db|_] when is_map(Db) ->
							 build_mapping(SchemaName, Attrs);
						 _ ->
							 #{type => TempType}
					 end;
				 TempType == object_list ->
					 FieldType = normalize_type(TempType),
					 Attrs = sumo_internal:field_attrs(Field),
					 case Attrs of
						 [] -> #{type => FieldType, enabled => false};
						 [Db|_] when is_map(Db) ->
							 maps:merge(#{type => FieldType}, build_mapping(SchemaName, Attrs));
						 _ ->
							 #{type => FieldType
								 ,enabled => false}
					 end;
				 true ->
					 FieldType = normalize_type(TempType),
					 #{type => FieldType}
			end,
			maps:put(Name, FullFieldType, Acc)
	end,
	Properties = lists:foldl(Fun, #{}, Fields),
	#{'properties' => Properties}.
%io:format("Properties: ~p~n",[Properties]),
%maps:from_list([#{'mappings' => #{'_doc' => #{properties => Properties}}}, #{'settings' => #{index =>  #{number_of_shards =>  1, number_of_replicas => 1}}}]).
%maps:from_list([{'mappings' , #{'_doc' => #{properties => Properties}}},{'settings' , #{index =>  #{number_of_shards =>  1, number_of_replicas => 1}}}]).
% https://www.elastic.co/guide/en/elasticsearch/reference/6.4/sql-data-types.html

normalize_type(date) -> date;
normalize_type(datetime) -> date;
normalize_type(custom) -> text;
normalize_type(string) -> text;
normalize_type(binary) -> keyword;
normalize_type(object_list) -> nested;

%normalize_type(_Type) -> text.
% keyword, boolean, float, integer, nested, object, geo_point
normalize_type(Type) -> Type.

%% @private
count(PoolName, Index, Type, Query, Params) ->
	{ok, #{<<"count">> := Count}} =
	tirerl:count(PoolName, Index, Type, Query, Params),
	Count.

%% @private
delete_by_query(PoolName, Index, Type, Query, Params) ->
	{ok, _} = tirerl:delete_by_query(PoolName, Index, Type, Query, Params),
	ok.

-spec sleep(term()) -> term().
%% @private
sleep(Doc) ->
	sumo_utils:doc_transform(fun sleep_fun/1, Doc).

%% @private

sleep_fun({FieldType, _, FieldValue}) when FieldType =:= datetime;
																					 FieldType =:= date ->
	case {FieldType, sumo_utils:is_datetime(FieldValue)} of
		{date, true}     -> iso8601:format({FieldValue, {0, 0, 0}});
		{datetime, true} -> iso8601:format(FieldValue);
		_                -> FieldValue
	end;
sleep_fun({_, _, undefined}) ->
	null;
sleep_fun({custom, _, FieldValue}) ->
	base64:encode(term_to_binary(FieldValue));
sleep_fun({_, _, FieldValue}) ->
	FieldValue.

% sleep_fun(FieldType, _, FieldValue, _) when FieldType =:= datetime;
%                                            FieldType =:= date ->
%     case {FieldType, sumo_utils:is_datetime(FieldValue)} of
%       {date, true}     -> iso8601:format({FieldValue, {0, 0, 0}});
%       {datetime, true} -> iso8601:format(FieldValue);
%       _                -> FieldValue
%     end;
% sleep_fun(_, _, undefined, _) ->
%     null;
% sleep_fun(custom, _, FieldValue, _) ->
%     base64:encode(term_to_binary(FieldValue));
% sleep_fun(_, _, FieldValue, _) ->
%     FieldValue.

%% @private
-spec wakeup(term()) -> term().
wakeup(Doc) ->
	sumo_utils:doc_transform(fun wakeup_fun/1, Doc).

%% @private
wakeup_fun({datetime, _, FieldValue}) ->
	FieldValue;
%zt_util:now_to_utc_binary(FieldValue);

wakeup_fun({date, _, FieldValue}) ->
	{Date, _} = iso8601:parse(FieldValue),
	Date;

wakeup_fun({_, _, null}) ->
	undefined;

wakeup_fun({custom, _, FieldValue}) ->
	binary_to_term(base64:decode(FieldValue));

wakeup_fun({_, _, FieldValue}) ->
	FieldValue.

% wakeup_fun(datetime, _, FieldValue, _) ->
%     iso8601:parse(FieldValue);
% wakeup_fun(date, _, FieldValue, _) ->
%     {Date, _} = iso8601:parse(FieldValue),
%     Date;
% wakeup_fun(_, _, null, _) ->
%     undefined;
% wakeup_fun(custom, _, FieldValue, _) ->
%     binary_to_term(base64:decode(FieldValue));
% wakeup_fun(_, _, FieldValue, _) ->
%     FieldValue.

