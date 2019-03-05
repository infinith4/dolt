package tblcmds

/*
func TestExportTblSchema(t *testing.T) {
	tests := []struct {
		table       string
		outFilePath string
	}{
		{
			tableName,
			"schema.json",
		},
	}

	for _, test := range tests {
		dEnv := createEnvWithSeedData(t)
		root, _ := dEnv.WorkingRoot()
		tbl, _ := root.GetTable(tableName)

		originalSchema := tbl.GetSchema()
		result := exportTblSchema(tableName, tbl, test.outFilePath, dEnv)

		if result != nil {
			t.Fatal("Unexpected failure.")
		}

		data, err := dEnv.FS.ReadFile(test.outFilePath)

		if err != nil {
			t.Fatal(err.Error())
		}

		newSchema, _ := jsonenc.SchemaFromJSON(data)

		if !reflect.DeepEqual(originalSchema, newSchema) {
			t.Error(originalSchema, "!=", newSchema)
		}
	}
}

func TestAddFieldToSchema(t *testing.T) {
	tests := []struct {
		tblName    string
		newColName string
		colType    string
		required   string
		defaultVal string
	}{
		{tableName, "date", "string", "false", ""},
		{tableName, "date", "string", "true", ""},
		{tableName, "date", "string", "true", "02-25-2019"},
		{tableName, "number", "int", "true", "5"},
	}
	for _, test := range tests {
		dEnv := createEnvWithSeedData(t)
		root, _ := dEnv.WorkingRoot()
		tbl, _ := root.GetTable(tableName)
		originalSchemaFields := tbl.GetSchema().GetFieldNames()

		result, err := addFieldToSchema(tableName, tbl, dEnv, test.newColName, test.colType, test.required, &test.defaultVal)
		if err != nil {
			t.Fatal(err.Error())
		}

		newSchema := result.GetSchema()
		newSchemaFields := newSchema.GetFieldNames()
		originalPlusNewField := append(originalSchemaFields, test.newColName)

		if !reflect.DeepEqual(originalPlusNewField, newSchemaFields) {
			t.Error(originalSchemaFields, "!=", newSchemaFields)
		}
	}

}

func TestRenameColumnOfSchema(t *testing.T) {
	tests := []struct {
		table         string
		oldName       string
		newName       string
		newFieldNames []string
	}{
		{tableName, "is_married", "married", []string{"id", "name", "age", "title", "married"}},
	}

	for _, test := range tests {
		dEnv := createEnvWithSeedData(t)
		root, _ := dEnv.WorkingRoot()
		tbl, _ := root.GetTable(tableName)

		result, err := renameColumnOfSchema(test.oldName, test.newName, tbl, dEnv)
		if err != nil {
			t.Fatal(err.Error())
		}

		newSchema := result.GetSchema()
		newSchemaFields := newSchema.GetFieldNames()

		if !reflect.DeepEqual(test.newFieldNames, newSchemaFields) {
			t.Error(test.newFieldNames, "!=", newSchemaFields)
		}
	}

}

func TestRemoveColumnFromTable(t *testing.T) {
	tests := []struct {
		table         string
		colName       string
		newFieldNames []string
	}{
		{tableName, "is_married", []string{"id", "name", "age", "title"}},
	}

	for _, test := range tests {
		dEnv := createEnvWithSeedData(t)
		root, _ := dEnv.WorkingRoot()
		tbl, _ := root.GetTable(tableName)

		result, err := removeColumnFromTable(tbl, test.colName, dEnv)
		if err != nil {
			t.Fatal(err.Error())
		}

		newSchema := result.GetSchema()
		newSchemaFields := newSchema.GetFieldNames()

		if !reflect.DeepEqual(test.newFieldNames, newSchemaFields) {
			t.Error(test.newFieldNames, "!=", newSchemaFields)
		}
	}
}
*/
