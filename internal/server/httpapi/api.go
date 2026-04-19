package httpapi

import (
	"encoding/json"
	"fmt"

	"github.com/correodabid/asql/internal/engine/parser/ast"
	adminapi "github.com/correodabid/asql/pkg/adminapi"
)

// The request/response types for the admin HTTP API live in pkg/adminapi so
// that external clients can depend on them without importing internal engine
// packages. The aliases below keep intra-package references short.

type (
	ExecuteRequest  = adminapi.ExecuteRequest
	ExecuteResponse = adminapi.ExecuteResponse

	BeginTxRequest  = adminapi.BeginTxRequest
	BeginTxResponse = adminapi.BeginTxResponse

	CommitTxRequest  = adminapi.CommitTxRequest
	CommitTxResponse = adminapi.CommitTxResponse

	RollbackTxRequest  = adminapi.RollbackTxRequest
	RollbackTxResponse = adminapi.RollbackTxResponse

	ExecuteBatchRequest  = adminapi.ExecuteBatchRequest
	ExecuteBatchResponse = adminapi.ExecuteBatchResponse

	QueryRequest  = adminapi.QueryRequest
	QueryResponse = adminapi.QueryResponse

	TimeTravelQueryRequest  = adminapi.TimeTravelQueryRequest
	TimeTravelQueryResponse = adminapi.TimeTravelQueryResponse

	RowHistoryRequest  = adminapi.RowHistoryRequest
	RowHistoryResponse = adminapi.RowHistoryResponse

	EntityVersionHistoryRequest  = adminapi.EntityVersionHistoryRequest
	EntityVersionHistoryEntry    = adminapi.EntityVersionHistoryEntry
	EntityVersionHistoryResponse = adminapi.EntityVersionHistoryResponse

	ExplainQueryRequest  = adminapi.ExplainQueryRequest
	ExplainQueryResponse = adminapi.ExplainQueryResponse

	ScanStrategyStatsRequest  = adminapi.ScanStrategyStatsRequest
	ScanStrategyStatsResponse = adminapi.ScanStrategyStatsResponse

	ReplayToLSNRequest  = adminapi.ReplayToLSNRequest
	ReplayToLSNResponse = adminapi.ReplayToLSNResponse

	EngineStatsRequest  = adminapi.EngineStatsRequest
	SystemInfoResponse  = adminapi.SystemInfoResponse
	EngineStatsResponse = adminapi.EngineStatsResponse

	EvaluateReadRouteRequest  = adminapi.EvaluateReadRouteRequest
	EvaluateReadRouteResponse = adminapi.EvaluateReadRouteResponse
	ReadRoutingStatsRequest   = adminapi.ReadRoutingStatsRequest
	ReadRoutingStatsResponse  = adminapi.ReadRoutingStatsResponse

	LeadershipStateRequest  = adminapi.LeadershipStateRequest
	LeadershipStateResponse = adminapi.LeadershipStateResponse

	SchemaSnapshotRequest     = adminapi.SchemaSnapshotRequest
	SchemaSnapshotColumn      = adminapi.SchemaSnapshotColumn
	SchemaSnapshotIndex       = adminapi.SchemaSnapshotIndex
	SchemaSnapshotVersionedFK = adminapi.SchemaSnapshotVersionedFK
	SchemaSnapshotTable       = adminapi.SchemaSnapshotTable
	SchemaSnapshotDomain      = adminapi.SchemaSnapshotDomain
	SchemaSnapshotEntity      = adminapi.SchemaSnapshotEntity
	SchemaSnapshotResponse    = adminapi.SchemaSnapshotResponse

	TimelineCommitsRequest      = adminapi.TimelineCommitsRequest
	TimelineCommitMutationEntry = adminapi.TimelineCommitMutationEntry
	TimelineCommitEntry         = adminapi.TimelineCommitEntry
	TimelineCommitsResponse     = adminapi.TimelineCommitsResponse

	TimelineEventsResponse = adminapi.TimelineEventsResponse
	TimelineSnapshotPoint  = adminapi.TimelineSnapshotPoint

	LastLSNResponse = adminapi.LastLSNResponse
)

// ─── Helpers ────────────────────────────────────────────────────────────────

// normalizeRows converts ast.Literal maps to generic interface{} maps for
// JSON serialization.
func normalizeRows(rows []map[string]ast.Literal) []map[string]interface{} {
	normalized := make([]map[string]interface{}, 0, len(rows))
	for _, row := range rows {
		converted := make(map[string]interface{}, len(row))
		for key, value := range row {
			switch value.Kind {
			case ast.LiteralString:
				converted[key] = value.StringValue
			case ast.LiteralNumber:
				converted[key] = value.NumberValue
			case ast.LiteralBoolean:
				converted[key] = value.BoolValue
			case ast.LiteralFloat:
				converted[key] = value.FloatValue
			case ast.LiteralTimestamp:
				converted[key] = value.NumberValue // keep as microsecond epoch
			case ast.LiteralJSON:
				var parsed interface{}
				if jsonErr := json.Unmarshal([]byte(value.StringValue), &parsed); jsonErr == nil {
					converted[key] = parsed
				} else {
					converted[key] = value.StringValue
				}
			case ast.LiteralNull:
				converted[key] = nil
			default:
				converted[key] = nil
			}
		}
		normalized = append(normalized, converted)
	}

	return normalized
}

// formatDefaultValue renders a DefaultExpr for JSON output.
func formatDefaultValue(def *ast.DefaultExpr) string {
	if def == nil {
		return ""
	}
	switch def.Kind {
	case ast.DefaultAutoIncrement:
		return "AUTOINCREMENT"
	case ast.DefaultUUIDv7:
		return "UUID_V7"
	case ast.DefaultLiteral:
		switch def.Value.Kind {
		case ast.LiteralNull:
			return "NULL"
		case ast.LiteralString:
			return "'" + def.Value.StringValue + "'"
		case ast.LiteralNumber:
			return fmt.Sprintf("%d", def.Value.NumberValue)
		case ast.LiteralBoolean:
			if def.Value.BoolValue {
				return "TRUE"
			}
			return "FALSE"
		case ast.LiteralFloat:
			return fmt.Sprintf("%g", def.Value.FloatValue)
		default:
			return ""
		}
	default:
		return ""
	}
}
