// Copyright 2020 Liquidata, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package doltdb

import (
	"context"
	"fmt"
	"sort"

	"github.com/liquidata-inc/dolt/go/store/marshal"
	"github.com/liquidata-inc/dolt/go/store/types"
)

type ForeignKeyCollection struct {
	foreignKeys map[string]*ForeignKey
}

type ForeignKeyReferenceOption byte

const (
	ForeignKeyReferenceOption_DefaultAction ForeignKeyReferenceOption = iota
	ForeignKeyReferenceOption_Cascade
	ForeignKeyReferenceOption_NoAction
	ForeignKeyReferenceOption_Restrict
	ForeignKeyReferenceOption_SetNull
)

type ForeignKey struct {
	Name                   string                    `noms:"name" json:"name"`
	TableName              string                    `noms:"tbl_name" json:"tbl_name"`
	TableIndex             string                    `noms:"tbl_index" json:"tbl_index"`
	TableColumns           []string                  `noms:"tbl_cols" json:"tbl_cols"`
	ReferencedTableName    string                    `noms:"ref_tbl_name" json:"ref_tbl_name"`
	ReferencedTableIndex   string                    `noms:"ref_tbl_index" json:"ref_tbl_index"`
	ReferencedTableColumns []string                  `noms:"ref_tbl_cols" json:"ref_tbl_cols"`
	OnUpdate               ForeignKeyReferenceOption `noms:"on_update" json:"on_update"`
	OnDelete               ForeignKeyReferenceOption `noms:"on_delete" json:"on_delete"`
}

// NewForeignKeyCollection returns a new ForeignKeyCollection using the provided map returned previously by GetMap.
func NewForeignKeyCollection(ctx context.Context, fkMap types.Map) (*ForeignKeyCollection, error) {
	fkc := &ForeignKeyCollection{
		foreignKeys: make(map[string]*ForeignKey),
	}
	err := fkMap.IterAll(ctx, func(_, value types.Value) error {
		foreignKey := &ForeignKey{}
		err := marshal.Unmarshal(ctx, fkMap.Format(), value, foreignKey)
		if err != nil {
			return err
		}
		fkc.foreignKeys[foreignKey.Name] = foreignKey
		return nil
	})
	if err != nil {
		return nil, err
	}
	return fkc, nil
}

// AddForeignKey adds the given foreign key to the collection. Checks that the given name is unique in the collection.
func (fkc *ForeignKeyCollection) AddForeignKey(name string, tableName string, tableIndex string, tableColumns []string, referencedTableName string,
	referencedTableIndex string, referencedTableColumns []string, onDelete ForeignKeyReferenceOption, onUpdate ForeignKeyReferenceOption) (*ForeignKey, error) {
	if name == "" {
		name = fmt.Sprintf("fk_%s_%s_1", tableName, referencedTableName)
		for i := 2; fkc.Contains(name); i++ {
			name = fmt.Sprintf("fk_%s_%s_%d", tableName, referencedTableName, i)
		}
	} else if fkc.Contains(name) {
		return nil, fmt.Errorf("a foreign key with the name `%s` already exists", name)
	}

	foreignKey := &ForeignKey{
		Name:                   name,
		TableName:              tableName,
		TableIndex:             tableIndex,
		TableColumns:           tableColumns,
		ReferencedTableName:    referencedTableName,
		ReferencedTableIndex:   referencedTableIndex,
		ReferencedTableColumns: referencedTableColumns,
		OnUpdate:               onUpdate,
		OnDelete:               onDelete,
	}
	fkc.foreignKeys[name] = foreignKey
	return foreignKey, nil
}

// AllForeignKeys returns a sorted slice containing all of the foreign keys in this collection.
func (fkc *ForeignKeyCollection) AllForeignKeys() []*ForeignKey {
	fks := make([]*ForeignKey, len(fkc.foreignKeys))
	i := 0
	for _, fk := range fkc.foreignKeys {
		fks[i] = fk
		i++
	}
	sort.Slice(fks, func(i, j int) bool {
		return fks[i].Name < fks[j].Name
	})
	return fks
}

// Contains returns whether the given foreign key name already exists for this collection.
func (fkc *ForeignKeyCollection) Contains(foreignKeyName string) bool {
	_, ok := fkc.foreignKeys[foreignKeyName]
	return ok
}

// Count returns the number of indexes in this collection.
func (fkc *ForeignKeyCollection) Count() int {
	return len(fkc.foreignKeys)
}

// Get returns the foreign key with the given name, or nil if it does not exist.
func (fkc *ForeignKeyCollection) Get(foreignKeyName string) *ForeignKey {
	fk, ok := fkc.foreignKeys[foreignKeyName]
	if ok {
		return fk
	}
	return nil
}

// Map returns the collection as a Noms Map for persistence.
func (fkc *ForeignKeyCollection) Map(ctx context.Context, vrw types.ValueReadWriter) (types.Map, error) {
	fkMap, err := types.NewMap(ctx, vrw)
	if err != nil {
		return types.EmptyMap, err
	}
	fkMapEditor := fkMap.Edit()
	for _, foreignKey := range fkc.foreignKeys {
		val, err := marshal.Marshal(ctx, vrw, *foreignKey)
		if err != nil {
			return types.EmptyMap, err
		}
		fkMapEditor.Set(types.String(foreignKey.Name), val)
	}
	return fkMapEditor.Map(ctx)
}

// RelevantForeignKeys returns all foreign keys that reference the given table in some capacity. The returned array
// declaresFk contains all foreign keys in which this table declared the foreign key. The array referencedByFk contains
// all foreign keys in which this table is the referenced table. If the table contains a self-referential foreign key,
// it will be present in both declaresFk and referencedByFk. Each array is sorted.
func (fkc *ForeignKeyCollection) RelevantForeignKeys(tableName string) (declaresFk, referencedByFk []*ForeignKey) {
	for _, foreignKey := range fkc.foreignKeys {
		if foreignKey.TableName == tableName {
			declaresFk = append(declaresFk, foreignKey)
		}
		if foreignKey.ReferencedTableName == tableName {
			referencedByFk = append(referencedByFk, foreignKey)
		}
	}
	sort.Slice(declaresFk, func(i, j int) bool {
		return declaresFk[i].Name < declaresFk[j].Name
	})
	sort.Slice(referencedByFk, func(i, j int) bool {
		return referencedByFk[i].Name < referencedByFk[j].Name
	})
	return
}

// RemoveForeignKey removes a foreign key from the collection. It does not remove the associated indexes from their
// respective tables.
func (fkc *ForeignKeyCollection) RemoveForeignKey(foreignKeyName string) (*ForeignKey, error) {
	fk, ok := fkc.foreignKeys[foreignKeyName]
	if !ok {
		return nil, fmt.Errorf("`%s` does not exist as a foreign key", foreignKeyName)
	}
	delete(fkc.foreignKeys, foreignKeyName)
	return fk, nil
}

// RenameTable updates all foreign key entries in the collection with the updated table name. Does not check for name
// collisions.
func (fkc *ForeignKeyCollection) RenameTable(oldTableName, newTableName string) {
	for _, foreignKey := range fkc.foreignKeys {
		if foreignKey.TableName == oldTableName {
			foreignKey.TableName = newTableName
		}
		if foreignKey.ReferencedTableName == oldTableName {
			foreignKey.ReferencedTableName = newTableName
		}
	}
}

// RenameColumn updates all foreign key entries in the collection with the updated column name for the given table. Does
// not check for name collisions.
func (fkc *ForeignKeyCollection) RenameColumn(tableName, oldColumnName, newColumnName string) {
	for _, foreignKey := range fkc.foreignKeys {
		if foreignKey.TableName == tableName {
			for i, columnName := range foreignKey.TableColumns {
				if columnName == oldColumnName {
					foreignKey.TableColumns[i] = newColumnName
				}
			}
		}
		if foreignKey.ReferencedTableName == tableName {
			for i, columnName := range foreignKey.ReferencedTableColumns {
				if columnName == oldColumnName {
					foreignKey.ReferencedTableColumns[i] = newColumnName
				}
			}
		}
	}
}

func (refOp ForeignKeyReferenceOption) String() string {
	switch refOp {
	case ForeignKeyReferenceOption_DefaultAction:
		return "NONE SPECIFIED"
	case ForeignKeyReferenceOption_Cascade:
		return "CASCADE"
	case ForeignKeyReferenceOption_NoAction:
		return "NO ACTION"
	case ForeignKeyReferenceOption_Restrict:
		return "RESTRICT"
	case ForeignKeyReferenceOption_SetNull:
		return "SET DEFAULT"
	default:
		return "INVALID"
	}
}
