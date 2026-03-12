package adminapi

import internalapi "asql/internal/server/httpapi"

type ExecuteRequest = internalapi.ExecuteRequest
type ExecuteResponse = internalapi.ExecuteResponse

type BeginTxRequest = internalapi.BeginTxRequest
type BeginTxResponse = internalapi.BeginTxResponse

type CommitTxRequest = internalapi.CommitTxRequest
type CommitTxResponse = internalapi.CommitTxResponse

type RollbackTxRequest = internalapi.RollbackTxRequest
type RollbackTxResponse = internalapi.RollbackTxResponse

type ExecuteBatchRequest = internalapi.ExecuteBatchRequest
type ExecuteBatchResponse = internalapi.ExecuteBatchResponse

type QueryRequest = internalapi.QueryRequest
type QueryResponse = internalapi.QueryResponse

type TimeTravelQueryRequest = internalapi.TimeTravelQueryRequest
type TimeTravelQueryResponse = internalapi.TimeTravelQueryResponse

type RowHistoryRequest = internalapi.RowHistoryRequest
type RowHistoryResponse = internalapi.RowHistoryResponse

type EntityVersionHistoryRequest = internalapi.EntityVersionHistoryRequest
type EntityVersionHistoryEntry = internalapi.EntityVersionHistoryEntry
type EntityVersionHistoryResponse = internalapi.EntityVersionHistoryResponse

type ExplainQueryRequest = internalapi.ExplainQueryRequest
type ExplainQueryResponse = internalapi.ExplainQueryResponse

type ScanStrategyStatsRequest = internalapi.ScanStrategyStatsRequest
type ScanStrategyStatsResponse = internalapi.ScanStrategyStatsResponse

type ReplayToLSNRequest = internalapi.ReplayToLSNRequest
type ReplayToLSNResponse = internalapi.ReplayToLSNResponse

type EngineStatsRequest = internalapi.EngineStatsRequest
type SystemInfoResponse = internalapi.SystemInfoResponse
type EngineStatsResponse = internalapi.EngineStatsResponse

type EvaluateReadRouteRequest = internalapi.EvaluateReadRouteRequest
type EvaluateReadRouteResponse = internalapi.EvaluateReadRouteResponse
type ReadRoutingStatsRequest = internalapi.ReadRoutingStatsRequest
type ReadRoutingStatsResponse = internalapi.ReadRoutingStatsResponse

type LeadershipStateRequest = internalapi.LeadershipStateRequest
type LeadershipStateResponse = internalapi.LeadershipStateResponse

type SchemaSnapshotRequest = internalapi.SchemaSnapshotRequest
type SchemaSnapshotColumn = internalapi.SchemaSnapshotColumn
type SchemaSnapshotIndex = internalapi.SchemaSnapshotIndex
type SchemaSnapshotVersionedFK = internalapi.SchemaSnapshotVersionedFK
type SchemaSnapshotTable = internalapi.SchemaSnapshotTable
type SchemaSnapshotDomain = internalapi.SchemaSnapshotDomain
type SchemaSnapshotEntity = internalapi.SchemaSnapshotEntity
type SchemaSnapshotResponse = internalapi.SchemaSnapshotResponse

type TimelineCommitsRequest = internalapi.TimelineCommitsRequest
type TimelineCommitMutationEntry = internalapi.TimelineCommitMutationEntry
type TimelineCommitEntry = internalapi.TimelineCommitEntry
type TimelineCommitsResponse = internalapi.TimelineCommitsResponse

type LastLSNResponse = internalapi.LastLSNResponse
