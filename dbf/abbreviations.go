package dbf

import (
	"errors"
	"fmt"
	"github.com/LindsayBradford/go-dbf/godbf"
)

const (
	SocrNameField = "SOCRNAME"
	ScNameField   = "SCNAME"
)

// Reads abbreviations. Returns map (key is abbreviation, value is full name of address object)
func ReadAbbreviations(filePath string) (map[string]string, error) {
	dbfTable, err := godbf.NewFromFile(filePath, "866")
	if err != nil || dbfTable == nil {
		return nil, errors.New(fmt.Sprintf("error while reading dbf file %s", filePath))
	}
	result := make(map[string]string)
	for i := 0; i < dbfTable.NumberOfRecords(); i++ {
		value, valueErr := dbfTable.FieldValueByName(i, SocrNameField)
		key, keyErr := dbfTable.FieldValueByName(i, ScNameField)
		if valueErr != nil || keyErr != nil {
			continue
		}
		result[key] = value
	}
	return result, nil
}
