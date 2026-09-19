package inflearn

import (
	"strconv"
	"strings"
)

// publicationCandidatePreflightSQL is the runtime copy of Statground_SQL
// Clickhouse/Lecture_Publication/002a_inflearn_public_catalog_candidate_preflight.sql.
// It is read-only and runs after the three complete endpoint-local snapshots,
// before any completion marker or activation append.
const publicationCandidatePreflightSQL = `WITH
candidate AS
(
    SELECT tupleElement(v,1) AS surface,tupleElement(v,2) AS generation
    FROM
    (
      SELECT arrayJoin([
        tuple(toLowCardinality('webr'),
              fromUnixTimestamp64Milli(__WEBR_GENERATION_MS__)),
        tuple(toLowCardinality('mirtype'),
              fromUnixTimestamp64Milli(__MIRTYPE_GENERATION_MS__)),
        tuple(toLowCardinality('statground'),
              fromUnixTimestamp64Milli(__STATGROUND_GENERATION_MS__))
      ]) AS v
    )
),
previous AS
(
    SELECT
      surface AS previous_surface,
      generation AS previous_generation,
      source_fetched_max AS previous_source_fetched_max
    FROM lecture_publication.v_inflearn_public_catalog_generation_latest
),
lease AS
(
    SELECT * FROM lecture_publication.v_inflearn_public_catalog_publish_lease_current
    WHERE fence_epoch=toUInt64(__FENCE_EPOCH__)
      AND lease_uuid=toUUID('__LEASE_UUID__')
),
lease_endpoints AS
(
    SELECT hostName() AS endpoint,count() AS lease_rows,
      uniqExact(tuple(lease_uuid,writer_id,acquired_at,expires_at)) AS versions
    FROM clusterAllReplicas('statground_cluster','lecture_publication',
                            'inflearn_public_catalog_publish_lease_local')
    WHERE fence_epoch=toUInt64(__FENCE_EPOCH__)
      AND lease_uuid=toUUID('__LEASE_UUID__')
    GROUP BY endpoint
),
source_authority_fence_rows AS
(
    SELECT hostName() AS endpoint,authority_revision,operation_uuid,writer_id,recorded_at
    FROM clusterAllReplicas('statground_cluster','lecture_publication',
                            'inflearn_public_catalog_source_authority_fence_local')
),
source_authority_max AS
(
    SELECT endpoint,max(authority_revision) AS authority_revision
    FROM source_authority_fence_rows GROUP BY endpoint
),
source_authority_endpoints AS
(
    SELECT m.endpoint,m.authority_revision,
      countIf(r.authority_revision=m.authority_revision) AS fence_rows,
      uniqExactIf(tuple(r.operation_uuid,r.writer_id,r.recorded_at),
                  r.authority_revision=m.authority_revision) AS versions
    FROM source_authority_max AS m
    INNER JOIN source_authority_fence_rows AS r USING(endpoint)
    GROUP BY m.endpoint,m.authority_revision
),
source_authority AS
(
    SELECT count() AS endpoint_count,uniqExact(authority_revision) AS revision_count,
      min(authority_revision) AS authority_revision,
      countIf(fence_rows=1 AND versions=1) AS exact_endpoints
    FROM source_authority_endpoints
),
replica_inventory AS
(
    SELECT hostName() AS endpoint,database,table,zookeeper_path,is_readonly,
           is_session_expired,queue_size,future_parts,parts_to_check,
           lost_part_count,log_pointer,log_max_index,active_replicas,total_replicas
    FROM clusterAllReplicas('statground_cluster','system','replicas')
    WHERE (database,table) IN
    (
      ('webr_lecture','inflearn_r_lecture_catalog_local'),
      ('mirtype_lecture','inflearn_language_lecture_catalog_local'),
      ('Data_Lecture_Inflearn_Service','inflearn_course_dim_local'),
      ('Data_Lecture_Inflearn_Service','inflearn_course_display_translation_local'),
      ('Data_Lecture_Inflearn_Service','inflearn_course_price_fact_local'),
      ('webr_lecture','inflearn_r_lecture_catalog_serving_local'),
      ('mirtype_lecture','inflearn_language_lecture_catalog_serving_local'),
      ('statground_lecture','inflearn_workbench_catalog_serving_local'),
      ('lecture_publication','inflearn_public_catalog_publish_lease_local'),
      ('lecture_publication','inflearn_public_catalog_generation_local'),
      ('lecture_publication','inflearn_public_catalog_activation_local'),
      ('lecture_publication','inflearn_public_catalog_source_authority_fence_local'),
      ('lecture_publication','inflearn_public_catalog_source_authority_local')
    )
),
replicas AS
(
    SELECT count() AS row_count,uniqExact(endpoint) AS endpoint_count,
      uniqExactIf(zookeeper_path,table IN
        ('inflearn_r_lecture_catalog_local','inflearn_language_lecture_catalog_local',
         'inflearn_course_dim_local','inflearn_course_display_translation_local',
         'inflearn_course_price_fact_local')) AS raw_groups,
      uniqExactIf(zookeeper_path,endsWith(table,'_serving_local')) AS snapshot_groups,
      uniqExactIf(zookeeper_path,database='lecture_publication') AS authority_groups,
      countIf(is_readonly!=0 OR is_session_expired!=0 OR queue_size!=0 OR future_parts!=0
        OR parts_to_check!=0 OR lost_part_count!=0 OR log_pointer<log_max_index
        OR (table IN ('inflearn_r_lecture_catalog_local','inflearn_language_lecture_catalog_local',
                      'inflearn_course_dim_local','inflearn_course_display_translation_local',
                      'inflearn_course_price_fact_local')
            AND (active_replicas!=2 OR total_replicas!=2))
        OR (table NOT IN ('inflearn_r_lecture_catalog_local','inflearn_language_lecture_catalog_local',
                          'inflearn_course_dim_local','inflearn_course_display_translation_local',
                          'inflearn_course_price_fact_local')
            AND (active_replicas!=4 OR total_replicas!=4))) AS unhealthy_rows
    FROM replica_inventory
),
queue AS
(
    SELECT countIf((database,table) IN
      (('webr_lecture','inflearn_r_lecture_catalog'),
       ('mirtype_lecture','inflearn_language_lecture_catalog'),
       ('webr_lecture','inflearn_r_lecture_catalog_serving'),
       ('mirtype_lecture','inflearn_language_lecture_catalog_serving'),
       ('statground_lecture','inflearn_workbench_catalog_serving'),
       ('lecture_publication','inflearn_public_catalog_source_authority_fence'),
       ('lecture_publication','inflearn_public_catalog_source_authority'),
       ('Data_Lecture_Inflearn_Service','inflearn_course_dim'),
       ('Data_Lecture_Inflearn_Service','inflearn_course_display_translation'),
       ('Data_Lecture_Inflearn_Service','inflearn_course_price_fact'))
      AND (data_files!=0 OR broken_data_files!=0 OR is_blocked!=0)) AS bad_rows
    FROM clusterAllReplicas('statground_cluster','system','distribution_queue')
),
raw_endpoints AS
(
    SELECT hostName() AS endpoint,getMacro('shard') AS shard_name,
      getMacro('replica') AS replica_name,c.surface,count() AS row_count,
      uniqExact(c.logical_key) AS logical_key_count,
      sumWithOverflow(c.row_fingerprint) AS fingerprint_sum,
      groupBitXor(c.row_fingerprint) AS fingerprint_xor
    FROM clusterAllReplicas('statground_cluster','lecture_publication',
                            'v_inflearn_public_catalog_candidate_local') AS c
    INNER JOIN candidate AS g ON g.surface=c.surface AND g.generation=c.generation
    GROUP BY endpoint,shard_name,replica_name,c.surface
),
raw_shards AS
(
    SELECT surface,shard_name,count() AS endpoint_count,
      uniqExact(replica_name) AS replica_count,
      uniqExact(tuple(row_count,logical_key_count,fingerprint_sum,fingerprint_xor)) AS versions,
      any(row_count) AS row_count,any(logical_key_count) AS logical_key_count,
      any(fingerprint_sum) AS fingerprint_sum,any(fingerprint_xor) AS fingerprint_xor
    FROM raw_endpoints GROUP BY surface,shard_name
),
statground_raw_endpoints AS
(
    SELECT hostName() AS endpoint,getMacro('shard') AS shard_name,
      getMacro('replica') AS replica_name,count() AS row_count,
      uniqExact(c.logical_key) AS logical_key_count,
      sumWithOverflow(c.row_fingerprint) AS fingerprint_sum,
      groupBitXor(c.row_fingerprint) AS fingerprint_xor,
      uniqExact(c.component) AS component_count
    FROM clusterAllReplicas('statground_cluster','lecture_publication',
                            'v_statground_inflearn_workbench_source_candidate_local') AS c
    GROUP BY endpoint,shard_name,replica_name
),
statground_raw_shards AS
(
    SELECT shard_name,count() AS endpoint_count,uniqExact(replica_name) AS replica_count,
      uniqExact(tuple(row_count,logical_key_count,fingerprint_sum,fingerprint_xor,component_count)) AS versions,
      any(row_count) AS row_count,any(logical_key_count) AS logical_key_count,
      any(fingerprint_sum) AS fingerprint_sum,any(fingerprint_xor) AS fingerprint_xor,
      any(component_count) AS component_count
    FROM statground_raw_endpoints GROUP BY shard_name
),
raw_physical AS
(
    SELECT surface,count() AS shard_count,sum(endpoint_count) AS endpoint_count,
      countIf(endpoint_count=2 AND replica_count=2 AND versions=1) AS peer_groups,
      sum(row_count) AS row_count,sum(logical_key_count) AS logical_key_count,
      sumWithOverflow(fingerprint_sum) AS fingerprint_sum,
      groupBitXor(fingerprint_xor) AS fingerprint_xor
    FROM raw_shards GROUP BY surface
    UNION ALL
    SELECT toLowCardinality('statground'),count(),sum(endpoint_count),
      countIf(endpoint_count=2 AND replica_count=2 AND versions=1 AND component_count=3),
      sum(row_count),sum(logical_key_count),sumWithOverflow(fingerprint_sum),groupBitXor(fingerprint_xor)
    FROM statground_raw_shards
),
raw_distributed AS
(
    SELECT c.surface,count() AS row_count,uniqExact(c.logical_key) AS logical_key_count,
      sumWithOverflow(c.row_fingerprint) AS fingerprint_sum,
      groupBitXor(c.row_fingerprint) AS fingerprint_xor
    FROM lecture_publication.v_inflearn_public_catalog_candidate AS c
    INNER JOIN candidate AS g ON g.surface=c.surface AND g.generation=c.generation
    GROUP BY c.surface
    UNION ALL
    SELECT toLowCardinality('statground'),count(),uniqExact(c.logical_key),
      sumWithOverflow(c.row_fingerprint),groupBitXor(c.row_fingerprint)
    FROM lecture_publication.v_statground_inflearn_workbench_source_candidate AS c
),
projection AS
(
    SELECT c.surface,count() AS row_count,uniqExact(c.logical_key) AS logical_key_count,
      sumWithOverflow(c.row_fingerprint) AS fingerprint_sum,
      groupBitXor(c.row_fingerprint) AS fingerprint_xor,
      max(c.source_fetched_at) AS source_fetched_max
    FROM lecture_publication.v_inflearn_public_catalog_projection_candidate AS c
    INNER JOIN candidate AS g ON g.surface=c.surface AND g.generation=c.generation
    GROUP BY c.surface
    UNION ALL
    SELECT toLowCardinality('statground'),count(),uniqExact(c.logical_key),
      sumWithOverflow(c.row_fingerprint),groupBitXor(c.row_fingerprint),
      max(c.source_fetched_at)
    FROM lecture_publication.v_statground_inflearn_workbench_projection_candidate AS c
),
snapshot_endpoints AS
(
    SELECT hostName() AS endpoint,c.surface,count() AS row_count,
      uniqExact(c.logical_key) AS logical_key_count,
      sumWithOverflow(c.row_fingerprint) AS fingerprint_sum,
      groupBitXor(c.row_fingerprint) AS fingerprint_xor,
      uniqExact(c.generation) AS generation_count,any(c.generation) AS generation
    FROM clusterAllReplicas('statground_cluster','lecture_publication',
                            'v_inflearn_public_catalog_projection_snapshot_local') AS c
    INNER JOIN candidate AS g ON g.surface=c.surface AND g.generation=c.generation
    GROUP BY endpoint,c.surface
),
snapshot AS
(
    SELECT surface,count() AS endpoint_rows,
      uniqExact(tuple(row_count,logical_key_count,fingerprint_sum,fingerprint_xor,generation_count,generation)) AS versions,
      any(row_count) AS row_count,any(logical_key_count) AS logical_key_count,
      any(fingerprint_sum) AS fingerprint_sum,any(fingerprint_xor) AS fingerprint_xor,
      countIf(generation_count=1) AS exact_generation_endpoints
    FROM snapshot_endpoints GROUP BY surface
),
proof AS
(
    SELECT g.surface,g.generation,
      r.row_count AS target_row_count,r.logical_key_count AS target_logical_key_count,
      r.fingerprint_sum AS target_fingerprint_sum,r.fingerprint_xor AS target_fingerprint_xor,
      p.row_count,p.logical_key_count,p.fingerprint_sum,p.fingerprint_xor,p.source_fetched_max,
      count() OVER () AS surface_count,
      countIf(r.shard_count=2 AND r.endpoint_count=4 AND r.peer_groups=2
        AND r.row_count>0 AND r.row_count=r.logical_key_count
        AND r.row_count=d.row_count AND r.logical_key_count=d.logical_key_count
        AND r.fingerprint_sum=d.fingerprint_sum AND r.fingerprint_xor=d.fingerprint_xor
        AND p.row_count>0 AND p.row_count=p.logical_key_count
        AND s.endpoint_rows=4 AND s.versions=1 AND s.exact_generation_endpoints=4
        AND s.row_count=p.row_count AND s.logical_key_count=p.logical_key_count
        AND s.fingerprint_sum=p.fingerprint_sum AND s.fingerprint_xor=p.fingerprint_xor
        AND dateDiff('second',p.source_fetched_max,now64(3,'Asia/Seoul')) BETWEEN 0 AND 129600
        AND (old.previous_surface IS NULL OR (g.generation>old.previous_generation
             AND p.source_fetched_max>old.previous_source_fetched_max))) OVER () AS matching_surface_count
    FROM candidate AS g
    INNER JOIN raw_physical AS r USING(surface)
    INNER JOIN raw_distributed AS d USING(surface)
    INNER JOIN projection AS p USING(surface)
    INNER JOIN snapshot AS s USING(surface)
    LEFT JOIN previous AS old ON old.previous_surface=g.surface
)
SELECT
    throwIf(
      (SELECT count() FROM candidate)!=3 OR (SELECT uniqExact(surface) FROM candidate)!=3
      OR ((SELECT count() FROM previous) NOT IN (0,3))
      OR (SELECT count() FROM lease)!=1
      OR (SELECT count() FROM lease_endpoints)!=4
      OR (SELECT countIf(lease_rows=1 AND versions=1) FROM lease_endpoints)!=4
      OR source_authority.endpoint_count!=4 OR source_authority.revision_count!=1
      OR source_authority.authority_revision=0 OR source_authority.exact_endpoints!=4
      OR replicas.row_count!=52 OR replicas.endpoint_count!=4 OR replicas.raw_groups!=10
      OR replicas.snapshot_groups!=3 OR replicas.authority_groups!=5 OR replicas.unhealthy_rows!=0
      OR queue.bad_rows!=0 OR (SELECT count() FROM raw_endpoints)!=8
      OR (SELECT count() FROM raw_shards)!=4 OR (SELECT count() FROM statground_raw_endpoints)!=4
      OR (SELECT count() FROM statground_raw_shards)!=2
      OR (SELECT countIf(component_count=3) FROM statground_raw_endpoints)!=4
      OR (SELECT count() FROM snapshot_endpoints)!=12
      OR surface_count!=3 OR matching_surface_count!=3,
      'Lecture candidate blocked before marker: fence, advancement, queue, 2x2 raw, Distributed, local snapshot, or source freshness proof failed'
    ) AS candidate_preflight_ok,
    surface,generation,target_row_count,target_logical_key_count,
    target_fingerprint_sum,target_fingerprint_xor,row_count,logical_key_count,
    fingerprint_sum,fingerprint_xor,source_fetched_max,source_authority.authority_revision
FROM proof
CROSS JOIN replicas
CROSS JOIN queue
CROSS JOIN source_authority
ORDER BY surface
SETTINGS skip_unavailable_shards=0,max_threads=1`

func renderPublicationCandidatePreflightSQL(generations map[string]int64, fenceEpoch uint64, leaseUUID string) string {
	replacements := map[string]string{
		"__WEBR_GENERATION_MS__":       strconv.FormatInt(generations["webr"], 10),
		"__MIRTYPE_GENERATION_MS__":    strconv.FormatInt(generations["mirtype"], 10),
		"__STATGROUND_GENERATION_MS__": strconv.FormatInt(generations["statground"], 10),
		"__FENCE_EPOCH__":              strconv.FormatUint(fenceEpoch, 10),
		"__LEASE_UUID__":               strings.ToLower(leaseUUID),
	}
	rendered := publicationCandidatePreflightSQL
	for token, value := range replacements {
		rendered = strings.ReplaceAll(rendered, token, value)
	}
	return rendered
}
