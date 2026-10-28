# Reconciliation report

Overall: **PASS**  |  events=60  sessions=11

| check | result | detail |
|---|---|---|
| event_id_unique | PASS | 60 events |
| sum(session.event_count) == count(events_clean) | PASS | 60 vs 60 |
| every_event_has_a_session | PASS | events with NULL session_id = 0 |
| session_count_and_bounds_match_events | PASS | mismatching sessions = 0 |
| gap_rule_holds_(window-function check) | PASS | violations = 0 |
| referential_integrity_events->dim_users | PASS | orphans = 0 |
| per_run_source_to_target_counts | PASS | unreconciled runs = [] |

## Rejects by reason

| reason | count |
|---|---|
| duplicate_event | 1 |
| invalid_event_type | 1 |
| invalid_reference_key | 1 |
| invalid_timestamp | 1 |
| malformed_record | 1 |
| missing_required_field | 1 |
| missing_timestamp | 1 |

## Ground-truth comparison (generator labels)

- sessions match expected: **True** (11 actual / 11 expected)
- rejects-by-reason match expected: **True**
