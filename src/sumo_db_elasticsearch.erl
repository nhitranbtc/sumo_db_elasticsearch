-module(sumo_db_elasticsearch).

-behaviour(application).

-export([start/0, start/2, stop/1]).

-spec start() -> {ok, [term()]}.
start() -> {ok, _} = 
start_deps(),
application:ensure_all_started(sumo_db_elasticsearch).
-spec start(StartType::application:start_type(), StartArgs::term()) ->
  {ok, pid()}.
start(_StartType, _StartArgs) -> 
start_deps(),
{ok, self()}.

-spec stop(State::term()) -> ok.
stop(_State) -> ok.

start_deps()->
lager:debug("ensure start tirerl ~n",[]),
    _ = [wh_util:ensure_started(App) || App <-[tirerl]],
    ok.

