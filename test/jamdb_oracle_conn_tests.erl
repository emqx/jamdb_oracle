-module(jamdb_oracle_conn_tests).

-include_lib("eunit/include/eunit.hrl").

%% Verifies placeholder extraction for SQL shapes commonly generated from EMQX
%% Oracle actions.  These tests document the expected bind order for DML
%% statements before map bind values are converted to positional binds.
bind_param_names_action_sqls_test_() ->
    [
        {"default insert", fun() ->
            assert_bind_param_names(
                ["1", "2", "3", "4"],
                "insert into t_mqtt_msgs(msgid, topic, qos, payload) "
                "values (:1, :2, :3, :4)"
            )
        end},
        {"bridge insert", fun() ->
            assert_bind_param_names(
                ["1", "2", "3", "4"],
                "INSERT INTO mqtt_test(topic, msgid, payload, retain) "
                "VALUES (:1, :2, :3, :4)"
            )
        end},
        {"update", fun() ->
            assert_bind_param_names(
                ["1", "2", "3", "4"],
                "UPDATE mqtt_test SET payload = :1, retain = :2 "
                "WHERE topic = :3 AND msgid = :4"
            )
        end},
        {"delete", fun() ->
            assert_bind_param_names(
                ["1", "2"],
                "DELETE FROM mqtt_test WHERE topic = :1 AND msgid = :2"
            )
        end},
        {"merge", fun() ->
            assert_bind_param_names(
                ["1", "2", "3", "4"],
                "MERGE INTO mqtt_test dst "
                "USING (SELECT :1 topic, :2 msgid, :3 payload, :4 retain FROM dual) src "
                "ON (dst.topic = src.topic AND dst.msgid = src.msgid) "
                "WHEN MATCHED THEN UPDATE SET dst.payload = src.payload, dst.retain = src.retain "
                "WHEN NOT MATCHED THEN INSERT (topic, msgid, payload, retain) "
                "VALUES (src.topic, src.msgid, src.payload, src.retain)"
            )
        end}
    ].

%% Verifies scanner boundaries that must not be treated as bind placeholders,
%% such as quoted strings, quoted identifiers, comments, q-quoted literals, and
%% PL/SQL assignment syntax.
bind_param_names_scanner_edges_test_() ->
    [
        {"named bind order", fun() ->
            assert_bind_param_names(
                ["id", "payload", "id", "tag", "tag"],
                "select :id, :payload, :id from dual where :tag = :tag"
            )
        end},
        {"quoted and commented text", fun() ->
            assert_bind_param_names(
                ["one", "one"],
                "select :one one from dual \"D:UAL\" \"D\"\":UAL\" -- :line_ignored\n"
                "where ':string_ignored' = 'It''s :still_ignored' "
                "and 1=:one /* :block_ignored */"
            )
        end},
        {"line comment ended by carriage return", fun() ->
            assert_bind_param_names(
                ["one", "one"],
                "select :one one from dual -- :line_ignored\r"
                "where 1=:one"
            )
        end},
        {"q quoted text", fun() ->
            assert_bind_param_names(
                ["one", "one"],
                "select :one one from dual where "
                "q'[I'm :not_a_bind]' = q'[I'm :not_a_bind]' and "
                "q'{:brace_ignored}' = q'{:brace_ignored}' and "
                "q'(:paren_ignored)' = q'(:paren_ignored)' and "
                "Q'<:angle_ignored>' = Q'<:angle_ignored>' and "
                "q'!:custom_ignored!' = q'!:custom_ignored!' and "
                "1=:one"
            )
        end},
        {"plsql assignment", fun() ->
            assert_bind_param_names(
                ["value"],
                "declare x number; begin x := :value; end;"
            )
        end},
        {"oracle bind name chars", fun() ->
            assert_bind_param_names(
                ["a_1", "b$2", "c#3"],
                "select :a_1, :b$2, :c#3 from dual"
            )
        end},
        {"empty bind name after colon", fun() ->
            assert_bind_param_names(
                [],
                "select : from dual"
            )
        end}
    ].

assert_bind_param_names(Expected, Query) ->
    ?assertEqual(Expected, jamdb_oracle_conn:bind_param_names(Query)).
