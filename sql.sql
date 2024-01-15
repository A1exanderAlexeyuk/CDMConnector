CREATE TEMPORARY TABLE Codesets (
  codeset_id int NOT NULL,
  concept_id bigint NOT NULL
)
;
INSERT INTO Codesets (codeset_id, concept_id)
SELECT 0 as codeset_id, c.concept_id FROM (select distinct I.concept_id FROM
( 
  select concept_id from "OMOP_SYNTHETIC_DATASET"."CDM53".CONCEPT where concept_id in (192671)
UNION  select c.concept_id
  from "OMOP_SYNTHETIC_DATASET"."CDM53".CONCEPT c
  join "OMOP_SYNTHETIC_DATASET"."CDM53".CONCEPT_ANCESTOR ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (192671)
  and c.invalid_reason is null
) I
) C;
CREATE TABLE qualified_events
 AS
SELECT
event_id, person_id, start_date, end_date, op_start_date, op_end_date, visit_occurrence_id
FROM
(
  select pe.event_id, pe.person_id, pe.start_date, pe.end_date, pe.op_start_date, pe.op_end_date, row_number() over (partition by pe.person_id order by pe.start_date ASC) as ordinal, cast(pe.visit_occurrence_id as bigint) as visit_occurrence_id
  FROM (
select P.ordinal as event_id, P.person_id, P.start_date, P.end_date, op_start_date, op_end_date, cast(P.visit_occurrence_id as bigint) as visit_occurrence_id
FROM
(
  select E.person_id, E.start_date, E.end_date,
         row_number() OVER (PARTITION BY E.person_id ORDER BY E.sort_date ASC, E.event_id) ordinal,
         OP.observation_period_start_date as op_start_date, OP.observation_period_end_date as op_end_date, cast(E.visit_occurrence_id as bigint) as visit_occurrence_id
  FROM 
  (
SELECT C.person_id, C.condition_occurrence_id as event_id, C.start_date, C.end_date,
  C.visit_occurrence_id, C.start_date as sort_date
FROM 
(
  SELECT co.person_id,co.condition_occurrence_id,co.condition_concept_id,co.visit_occurrence_id,co.condition_start_date as start_date, COALESCE(co.condition_end_date, DATEADD(day,1,co.condition_start_date)) as end_date 
  FROM "OMOP_SYNTHETIC_DATASET"."CDM53".CONDITION_OCCURRENCE co
  JOIN Codesets cs on (co.condition_concept_id = cs.concept_id and cs.codeset_id = 0)
) C
  ) E
	JOIN "OMOP_SYNTHETIC_DATASET"."CDM53".observation_period OP on E.person_id = OP.person_id and E.start_date >=  OP.observation_period_start_date and E.start_date <= op.observation_period_end_date
  WHERE DATEADD(day,0,OP.OBSERVATION_PERIOD_START_DATE) <= E.START_DATE AND DATEADD(day,0,E.START_DATE) <= OP.OBSERVATION_PERIOD_END_DATE
) P
) pe
) QE
;
CREATE TABLE Inclusion_0
 AS
SELECT
0 as inclusion_rule_id, person_id, event_id
FROM
(
  select pe.person_id, pe.event_id
  FROM qualified_events pe
JOIN (
select 0 as index_id, person_id, event_id
FROM
(
  select E.person_id, E.event_id 
  FROM qualified_events E
  INNER JOIN
  (
SELECT 0 as index_id, e.person_id, e.event_id
FROM qualified_events E
JOIN "OMOP_SYNTHETIC_DATASET"."CDM53".PERSON P ON P.PERSON_ID = E.PERSON_ID
WHERE P.gender_concept_id in (8507)
GROUP BY e.person_id, e.event_id
  ) CQ on E.person_id = CQ.person_id and E.event_id = CQ.event_id
  GROUP BY E.person_id, E.event_id
  HAVING COUNT(index_id) = 1
) G
) AC on AC.person_id = pe.person_id AND AC.event_id = pe.event_id
) Results
;
CREATE TABLE Inclusion_1
 AS
SELECT
1 as inclusion_rule_id, person_id, event_id
FROM
(
  select pe.person_id, pe.event_id
  FROM qualified_events pe
JOIN (
select 0 as index_id, person_id, event_id
FROM
(
  select E.person_id, E.event_id 
  FROM qualified_events E
  INNER JOIN
  (
select 0 as index_id, cc.person_id, cc.event_id
from (SELECT p.person_id, p.event_id 
FROM qualified_events P
JOIN (
select C.person_id, C.observation_period_id as event_id, C.start_date as start_date, C.end_date as end_date,
       CAST(NULL as bigint) as visit_occurrence_id, C.start_date as sort_date
from 
(
  select op.person_id,op.observation_period_id,op.period_type_concept_id,op.observation_period_start_date as start_date, op.observation_period_end_date as end_date , row_number() over (PARTITION BY op.person_id ORDER BY op.observation_period_start_date) as ordinal
  FROM "OMOP_SYNTHETIC_DATASET"."CDM53".OBSERVATION_PERIOD op
) C
) A on A.person_id = P.person_id  AND A.START_DATE >= P.OP_START_DATE AND A.START_DATE <= P.OP_END_DATE AND A.START_DATE >= P.OP_START_DATE AND A.START_DATE <= DATEADD(day,-30,P.START_DATE) AND A.END_DATE >= DATEADD(day,0,P.START_DATE) AND A.END_DATE <= P.OP_END_DATE ) cc 
GROUP BY cc.person_id, cc.event_id
HAVING COUNT(cc.event_id) >= 1
  ) CQ on E.person_id = CQ.person_id and E.event_id = CQ.event_id
  GROUP BY E.person_id, E.event_id
  HAVING COUNT(index_id) = 1
) G
) AC on AC.person_id = pe.person_id AND AC.event_id = pe.event_id
) Results
;
CREATE TABLE inclusion_events
 AS
SELECT
inclusion_rule_id, person_id, event_id
FROM
(select inclusion_rule_id, person_id, event_id from Inclusion_0
UNION ALL
select inclusion_rule_id, person_id, event_id from Inclusion_1) I;
TRUNCATE TABLE Inclusion_0;
DROP TABLE Inclusion_0;
TRUNCATE TABLE Inclusion_1;
DROP TABLE Inclusion_1;
CREATE TABLE included_events
 AS
SELECT
event_id, person_id, start_date, end_date, op_start_date, op_end_date
FROM
(
  SELECT event_id, person_id, start_date, end_date, op_start_date, op_end_date, row_number() over (partition by person_id order by start_date ASC) as ordinal
  from
  (
    select Q.event_id, Q.person_id, Q.start_date, Q.end_date, Q.op_start_date, Q.op_end_date, SUM(coalesce(POWER(cast(2 as bigint), I.inclusion_rule_id), 0)) as "inclusion_rule_mask"
    from qualified_events Q
    LEFT JOIN inclusion_events I on I.person_id = Q.person_id and I.event_id = Q.event_id
    GROUP BY Q.event_id, Q.person_id, Q.start_date, Q.end_date, Q.op_start_date, Q.op_end_date
  ) MG 
  WHERE (MG."inclusion_rule_mask" = POWER(cast(2 as bigint),2)-1)
) Results
WHERE Results.ordinal = 1
;
CREATE TABLE cohort_rows
 AS
SELECT
person_id, start_date, end_date
FROM
( 
	select F.person_id, F.start_date, F.end_date
	FROM (
	  select I.event_id, I.person_id, I.start_date, CE.end_date, row_number() over (partition by I.person_id, I.event_id order by CE.end_date) as ordinal
	  from included_events I
	  join ( 
select event_id, person_id, op_end_date as end_date from included_events
    ) CE on I.event_id = CE.event_id and I.person_id = CE.person_id and CE.end_date >= I.start_date
	) F
	WHERE F.ordinal = 1
) FE;
CREATE TABLE final_cohort
 AS
SELECT
person_id, min(start_date) as start_date, end_date
FROM
( 
	SELECT
		 c.person_id
		, c.start_date
		, MIN(ed.end_date) AS end_date
	FROM cohort_rows c
	JOIN ( 
    SELECT
      person_id
      , DATEADD(day,-1 * 0, event_date)  as end_date
    FROM
    (
      SELECT
        person_id
        , event_date
        , event_type
        , SUM(event_type) OVER (PARTITION BY person_id ORDER BY event_date, event_type ROWS UNBOUNDED PRECEDING) AS interval_status
      FROM
      (
        SELECT
          person_id
          , start_date AS event_date
          , -1 AS event_type
        FROM cohort_rows
        UNION ALL
        SELECT
          person_id
          , DATEADD(day,0,end_date) as end_date
          , 1 AS event_type
        FROM cohort_rows
      ) RAWDATA
    ) e
    WHERE interval_status = 0
  ) ed ON c.person_id = ed.person_id AND ed.end_date >= c.start_date
	GROUP BY c.person_id, c.start_date
) e
group by person_id, end_date
;
DELETE FROM "ATLAS"."RESULTS"."temp23019_chrt0" where "cohort_definition_id" = 3;
INSERT INTO "ATLAS"."RESULTS"."temp23019_chrt0" ("cohort_definition_id", "subject_id", "cohort_start_date", "cohort_end_date")
select 3 as "cohort_definition_id", person_id, start_date, end_date 
FROM final_cohort CO
;
delete from "ATLAS"."RESULTS"."temp23019_chrt0_censor_stats" where "cohort_definition_id" = 3;
CREATE TABLE inclusion_rules
 AS
SELECT
TRY_CAST(CAST("rule_sequence"  AS TEXT) AS int) as "rule_sequence"
FROM
(
  SELECT TRY_CAST(CAST(0  AS TEXT) AS int) as "rule_sequence" UNION ALL SELECT TRY_CAST(CAST(1  AS TEXT) AS int) as "rule_sequence"
) IR;
CREATE TABLE best_events
 AS
SELECT
q.person_id, q.event_id
FROM
qualified_events Q
join (
	SELECT R.person_id, R.event_id, ROW_NUMBER() OVER (PARTITION BY R.person_id ORDER BY R.rule_count DESC,R.min_rule_id ASC, R.start_date ASC) AS rank_value
	FROM (
		SELECT Q.person_id, Q.event_id, COALESCE(COUNT(DISTINCT I.inclusion_rule_id), 0) AS rule_count, COALESCE(MIN(I.inclusion_rule_id), 0) AS min_rule_id, Q.start_date
		FROM qualified_events Q
		LEFT JOIN inclusion_events I ON q.person_id = i.person_id AND q.event_id = i.event_id
		GROUP BY Q.person_id, Q.event_id, Q.start_date
	) R
) ranked on Q.person_id = ranked.person_id and Q.event_id = ranked.event_id
WHERE ranked.rank_value = 1
;
delete from "ATLAS"."RESULTS"."temp23019_chrt0_inclusion_result" where "cohort_definition_id" = 3 and "mode_id" = 0;
insert into "ATLAS"."RESULTS"."temp23019_chrt0_inclusion_result" ("cohort_definition_id", "inclusion_rule_mask", "person_count", "mode_id")
select 3 as "cohort_definition_id", "inclusion_rule_mask", COUNT(*) as "person_count", 0 as "mode_id"
from
(
  select Q.person_id, Q.event_id, CAST(SUM(coalesce(POWER(cast(2 as bigint), I.inclusion_rule_id), 0)) AS bigint) as "inclusion_rule_mask"
  from qualified_events Q
  LEFT JOIN inclusion_events I on q.person_id = i.person_id and q.event_id = i.event_id
  GROUP BY Q.person_id, Q.event_id
) MG 
group by "inclusion_rule_mask"
;
delete from "ATLAS"."RESULTS"."temp23019_chrt0_inclusion_stats" where "cohort_definition_id" = 3 and "mode_id" = 0;
insert into "ATLAS"."RESULTS"."temp23019_chrt0_inclusion_stats" ("cohort_definition_id", "rule_sequence", "person_count", "gain_count", "person_total", "mode_id")
select 3 as "cohort_definition_id", ir."rule_sequence", coalesce(T."person_count", 0) as "person_count", coalesce(SR."person_count", 0) "gain_count", EventTotal.total, 0 as "mode_id"
from inclusion_rules ir
left join
(
  select i.inclusion_rule_id, COUNT(i.event_id) as "person_count"
  from qualified_events Q
  JOIN inclusion_events i on Q.person_id = I.person_id and Q.event_id = i.event_id
  group by i.inclusion_rule_id
) T on ir."rule_sequence" = T.inclusion_rule_id
CROSS JOIN (select count(*) as total_rules from inclusion_rules) RuleTotal
CROSS JOIN (select COUNT(event_id) as total from qualified_events) EventTotal
LEFT JOIN "ATLAS"."RESULTS"."temp23019_chrt0_inclusion_result" SR on SR."mode_id" = 0 AND SR."cohort_definition_id" = 3 AND (POWER(cast(2 as bigint),RuleTotal.total_rules) - POWER(cast(2 as bigint),ir."rule_sequence") - 1) = SR."inclusion_rule_mask" 
;
delete from "ATLAS"."RESULTS"."temp23019_chrt0_summary_stats" where "cohort_definition_id" = 3 and "mode_id" = 0;
insert into "ATLAS"."RESULTS"."temp23019_chrt0_summary_stats" ("cohort_definition_id", "base_count", "final_count", "mode_id")
select 3 as "cohort_definition_id", PC.total as "person_count", coalesce(FC.total, 0) as "final_count", 0 as "mode_id"
FROM
(select COUNT(event_id) as total from qualified_events) PC,
(select sum(sr."person_count") as total
  from "ATLAS"."RESULTS"."temp23019_chrt0_inclusion_result" sr
  CROSS JOIN (select count(*) as total_rules from inclusion_rules) RuleTotal
  where sr."mode_id" = 0 and sr."cohort_definition_id" = 3 and sr."inclusion_rule_mask" = POWER(cast(2 as bigint),RuleTotal.total_rules)-1
) FC
;
delete from "ATLAS"."RESULTS"."temp23019_chrt0_inclusion_result" where "cohort_definition_id" = 3 and "mode_id" = 1;
insert into "ATLAS"."RESULTS"."temp23019_chrt0_inclusion_result" ("cohort_definition_id", "inclusion_rule_mask", "person_count", "mode_id")
select 3 as "cohort_definition_id", "inclusion_rule_mask", COUNT(*) as "person_count", 1 as "mode_id"
from
(
  select Q.person_id, Q.event_id, CAST(SUM(coalesce(POWER(cast(2 as bigint), I.inclusion_rule_id), 0)) AS bigint) as "inclusion_rule_mask"
  from best_events Q
  LEFT JOIN inclusion_events I on q.person_id = i.person_id and q.event_id = i.event_id
  GROUP BY Q.person_id, Q.event_id
) MG 
group by "inclusion_rule_mask"
;
delete from "ATLAS"."RESULTS"."temp23019_chrt0_inclusion_stats" where "cohort_definition_id" = 3 and "mode_id" = 1;
insert into "ATLAS"."RESULTS"."temp23019_chrt0_inclusion_stats" ("cohort_definition_id", "rule_sequence", "person_count", "gain_count", "person_total", "mode_id")
select 3 as "cohort_definition_id", ir."rule_sequence", coalesce(T."person_count", 0) as "person_count", coalesce(SR."person_count", 0) "gain_count", EventTotal.total, 1 as "mode_id"
from inclusion_rules ir
left join
(
  select i.inclusion_rule_id, COUNT(i.event_id) as "person_count"
  from best_events Q
  JOIN inclusion_events i on Q.person_id = I.person_id and Q.event_id = i.event_id
  group by i.inclusion_rule_id
) T on ir."rule_sequence" = T.inclusion_rule_id
CROSS JOIN (select count(*) as total_rules from inclusion_rules) RuleTotal
CROSS JOIN (select COUNT(event_id) as total from best_events) EventTotal
LEFT JOIN "ATLAS"."RESULTS"."temp23019_chrt0_inclusion_result" SR on SR."mode_id" = 1 AND SR."cohort_definition_id" = 3 AND (POWER(cast(2 as bigint),RuleTotal.total_rules) - POWER(cast(2 as bigint),ir."rule_sequence") - 1) = SR."inclusion_rule_mask" 
;
delete from "ATLAS"."RESULTS"."temp23019_chrt0_summary_stats" where "cohort_definition_id" = 3 and "mode_id" = 1;
insert into "ATLAS"."RESULTS"."temp23019_chrt0_summary_stats" ("cohort_definition_id", "base_count", "final_count", "mode_id")
select 3 as "cohort_definition_id", PC.total as "person_count", coalesce(FC.total, 0) as "final_count", 1 as "mode_id"
FROM
(select COUNT(event_id) as total from best_events) PC,
(select sum(sr."person_count") as total
  from "ATLAS"."RESULTS"."temp23019_chrt0_inclusion_result" sr
  CROSS JOIN (select count(*) as total_rules from inclusion_rules) RuleTotal
  where sr."mode_id" = 1 and sr."cohort_definition_id" = 3 and sr."inclusion_rule_mask" = POWER(cast(2 as bigint),RuleTotal.total_rules)-1
) FC
;
TRUNCATE TABLE best_events;
DROP TABLE best_events;
TRUNCATE TABLE inclusion_rules;
DROP TABLE inclusion_rules;
TRUNCATE TABLE cohort_rows;
DROP TABLE cohort_rows;
TRUNCATE TABLE final_cohort;
DROP TABLE final_cohort;
TRUNCATE TABLE inclusion_events;
DROP TABLE inclusion_events;
TRUNCATE TABLE qualified_events;
DROP TABLE qualified_events;
TRUNCATE TABLE included_events;
DROP TABLE included_events;
TRUNCATE TABLE Codesets;
DROP TABLE Codesets;
