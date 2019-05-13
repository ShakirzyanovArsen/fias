package dbf

import (
	"errors"
	"fmt"
	"github.com/LindsayBradford/go-dbf/godbf"
	"strconv"
)

type AddrObject struct {
	AoId       string
	AoGuid     string
	AoLevel    int
	FormalName string
	RegionCode int
	OffName    string
	PostalCode string
	TypeName   string
	ParentGuid string
}

const (
	AoIdField       = "AOID"
	AoGuidField     = "AOGUID"
	AoLevelField    = "AOLEVEL"
	FormalNameField = "FORMALNAME"
	RegionCodeField = "REGIONCODE"
	OffNameField    = "OFFNAME"
	PostalCodeField = "POSTALCODE"
	TypeNameField   = "SHORTNAME"
	ParentGuidField = "PARENTGUID"
	ActStatusField  = "ACTSTATUS"
	LiveStatusField = "LIVESTATUS"
)

//Reads AddrObjects from dbf file. Returns map, where key is aoguid of object and value is AddrObject.
//Function adds to map AddresObjects only live objects (actual status and live status = 1)
func ReadActualAddrObjects(filePath string) (map[string]*AddrObject, error) {
	dbfTable, err := godbf.NewFromFile(filePath, "866")
	if err != nil || dbfTable == nil {
		return nil, errors.New(fmt.Sprintf("error while reading dbf file %s", filePath))
	}
	var result = make(map[string]*AddrObject)
	for i := 0; i < dbfTable.NumberOfRecords(); i++ {
		actStatus, err := dbfTable.Int64FieldValueByName(i, ActStatusField)
		liveStatus, err := dbfTable.Int64FieldValueByName(i, LiveStatusField)
		if err != nil {
			return nil, err
		}
		if actStatus != 1 || liveStatus != 1 {
			continue
		}
		obj := AddrObject{}
		obj.AoId, _ = dbfTable.FieldValueByName(i, AoIdField)
		obj.AoGuid, _ = dbfTable.FieldValueByName(i, AoGuidField)
		aolevel, _ := dbfTable.FieldValueByName(i, AoLevelField)
		obj.AoLevel, _ = strconv.Atoi(aolevel)
		obj.FormalName, _ = dbfTable.FieldValueByName(i, FormalNameField)
		regionCodeStr, _ := dbfTable.FieldValueByName(i, RegionCodeField)
		obj.RegionCode, _ = strconv.Atoi(regionCodeStr)
		obj.OffName, _ = dbfTable.FieldValueByName(i, OffNameField)
		obj.PostalCode, _ = dbfTable.FieldValueByName(i, PostalCodeField)
		obj.TypeName, _ = dbfTable.FieldValueByName(i, TypeNameField)
		obj.ParentGuid, _ = dbfTable.FieldValueByName(i, ParentGuidField)
		result[obj.AoGuid] = &obj
	}
	return result, nil
}
