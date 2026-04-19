package studioapp

import (
	"fmt"
	"strings"

	api "github.com/correodabid/asql/pkg/adminapi"
)

type schemaLoadBaselineRequest struct {
	Domain string `json:"domain,omitempty"`
}

type schemaLoadBaselineResponse struct {
	Status   string           `json:"status"`
	Baseline schemaDDLRequest `json:"baseline"`
}

func schemaSnapshotToDDL(snapshot *api.SchemaSnapshotResponse, domain string) (schemaDDLRequest, error) {
	if snapshot == nil {
		return schemaDDLRequest{}, fmt.Errorf("schema snapshot response is required")
	}

	normalizedDomain, err := normalizeSchemaDomain(domain)
	if err != nil {
		return schemaDDLRequest{}, err
	}

	selected := api.SchemaSnapshotDomain{}
	found := false
	for _, candidate := range snapshot.Domains {
		if strings.EqualFold(strings.TrimSpace(candidate.Name), normalizedDomain) {
			selected = candidate
			found = true
			break
		}
	}
	if !found {
		if len(snapshot.Domains) == 0 {
			return schemaDDLRequest{Domain: normalizedDomain, Tables: []schemaDDLTable{}}, nil
		}
		selected = snapshot.Domains[0]
		normalizedDomain = strings.TrimSpace(selected.Name)
		if normalizedDomain == "" {
			normalizedDomain = domain
		}
	}

	tables := make([]schemaDDLTable, 0, len(selected.Tables))
	for _, table := range selected.Tables {
		columns := make([]schemaDDLColumn, 0, len(table.Columns))
		for _, column := range table.Columns {
			typeName := strings.TrimSpace(column.Type)
			if typeName == "" {
				typeName = "TEXT"
			}
			mapped := schemaDDLColumn{
				Name:       strings.TrimSpace(column.Name),
				Type:       strings.ToUpper(typeName),
				PrimaryKey: column.PrimaryKey,
				Unique:     column.Unique,
				Nullable:   !column.PrimaryKey,
			}
			if strings.TrimSpace(column.ReferencesTable) != "" && strings.TrimSpace(column.ReferencesColumn) != "" {
				mapped.References = &schemaDDLReference{Table: strings.TrimSpace(column.ReferencesTable), Column: strings.TrimSpace(column.ReferencesColumn)}
			}
			if dv := strings.TrimSpace(column.DefaultValue); dv != "" {
				mapped.DefaultValue = dv
			}
			columns = append(columns, mapped)
		}
		tables = append(tables, schemaDDLTable{Name: strings.TrimSpace(table.Name), Columns: columns, Indexes: snapshotIndexesToDDL(table.Indexes), VersionedForeignKeys: snapshotVersionedFKsToDDL(table.VersionedForeignKeys)})
	}

	return schemaDDLRequest{Domain: normalizedDomain, Tables: tables, Entities: snapshotEntitiesToDDL(selected.Entities)}, nil
}

type schemaLoadAllBaselinesResponse struct {
	Status    string             `json:"status"`
	Baselines []schemaDDLRequest `json:"baselines"`
}

func schemaSnapshotAllDomainsToDDL(snapshot *api.SchemaSnapshotResponse) ([]schemaDDLRequest, error) {
	if snapshot == nil {
		return nil, fmt.Errorf("schema snapshot response is required")
	}
	result := make([]schemaDDLRequest, 0, len(snapshot.Domains))
	for _, domain := range snapshot.Domains {
		domainName := strings.TrimSpace(domain.Name)
		if domainName == "" {
			continue
		}
		single := &api.SchemaSnapshotResponse{
			Status:  snapshot.Status,
			Domains: []api.SchemaSnapshotDomain{domain},
		}
		ddl, err := schemaSnapshotToDDL(single, domainName)
		if err != nil {
			return nil, err
		}
		result = append(result, ddl)
	}
	return result, nil
}

func snapshotIndexesToDDL(indexes []api.SchemaSnapshotIndex) []schemaDDLIndex {
	if len(indexes) == 0 {
		return nil
	}
	result := make([]schemaDDLIndex, 0, len(indexes))
	for _, idx := range indexes {
		cols := make([]string, len(idx.Columns))
		copy(cols, idx.Columns)
		method := strings.ToLower(strings.TrimSpace(idx.Method))
		if method == "" {
			method = "btree"
		}
		result = append(result, schemaDDLIndex{
			Name:    strings.TrimSpace(idx.Name),
			Columns: cols,
			Method:  method,
		})
	}
	return result
}

func snapshotVersionedFKsToDDL(vfks []api.SchemaSnapshotVersionedFK) []schemaDDLVersionedFK {
	if len(vfks) == 0 {
		return nil
	}
	result := make([]schemaDDLVersionedFK, 0, len(vfks))
	for _, vfk := range vfks {
		result = append(result, schemaDDLVersionedFK{
			Column:           vfk.Column,
			LSNColumn:        vfk.LSNColumn,
			ReferencesDomain: vfk.ReferencesDomain,
			ReferencesTable:  vfk.ReferencesTable,
			ReferencesColumn: vfk.ReferencesColumn,
		})
	}
	return result
}

func snapshotEntitiesToDDL(entities []api.SchemaSnapshotEntity) []schemaDDLEntity {
	if len(entities) == 0 {
		return nil
	}
	result := make([]schemaDDLEntity, 0, len(entities))
	for _, entity := range entities {
		tables := make([]string, len(entity.Tables))
		copy(tables, entity.Tables)
		result = append(result, schemaDDLEntity{
			Name:      entity.Name,
			RootTable: entity.RootTable,
			Tables:    tables,
		})
	}
	return result
}
