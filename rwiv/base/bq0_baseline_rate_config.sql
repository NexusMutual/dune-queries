-- RWIV Base Query 0 - Baseline Rate Config
-- Dune query ID: 7392734
-- Single-row source of truth for the baseline yield rate parameters.
--
-- Read by: BQ1 (Vault Daily Balance), Q1, Q3, Q4, Q4a, Q8, Q8a
-- Transitively read by: Q6, Q7, Q9 (via BQ1)
--
-- FIRST PASS SIMPLIFICATION: rate is hardcoded as a single segment.
-- When the first BaseRateChangeExecuted event fires, replace this with a
-- per-segment history sourced from rwivault_evt_baseratechangeexecuted, so
-- every consumer can compute rate(t) for any t.

SELECT
  CAST(1e18 AS double)                            AS start_rate,
  CAST(1000000001847694958 AS double) / 1e18      AS rate_per_second,
  CAST('2026-03-16 10:47:59' AS timestamp)        AS active_from
