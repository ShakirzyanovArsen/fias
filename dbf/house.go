package dbf

import (
	"github.com/LindsayBradford/go-dbf/godbf"
	"log"
	"time"
)

type House struct {
	AoGuid     string
	BuildNum   string
	StrucNum   string
	HouseGuid  string
	HouseId    string
	HouseNum   string
	PostalCode string
}

const (
	BuildNumField  = "BUILDNUM"
	StrucNumField  = "STRUCNUM"
	HouseGuidField = "HOUSEGUID"
	HouseIdField   = "HOUSEID"
	HouseNumField  = "HOUSENUM"
	EndDateField   = "ENDDATE"
	DateLayout     = "02.01.2006"
)

// Reads houses from dbf file. streets is map of street guids to ids.
// This function reads only actual houses which street guid exists in streets map
func ReadHouseChunks(filePath string, streets map[string]int) <-chan *House {
	dbfTable, err := godbf.NewFromFile(filePath, "866")
	if err != nil || dbfTable == nil {
		log.Printf("cant open file %s", filePath)
		return nil
	}
	ch := make(chan *House, 10000)
	go func(ch chan *House) {
		defer close(ch)
		for i := 0; i < dbfTable.NumberOfRecords(); i++ {
			house := House{}
			aoguid, err := dbfTable.FieldValueByName(i, AoGuidField)
			if err != nil {
				log.Printf("while fetch house row error occured(row skipped): %s", err)
				continue
			}
			endDateStr, _ := dbfTable.FieldValueByName(i, EndDateField)
			endDateStr = endDateStr[6:8] + "." + endDateStr[4:6] + "." + endDateStr[0:4]
			parsedEndDate, err := time.Parse(DateLayout, endDateStr)
			if err != nil || parsedEndDate.Before(time.Now()) {
				continue
			}
			if _, exists := streets[aoguid]; !exists {
				continue
			}
			house.AoGuid = aoguid
			house.BuildNum, _ = dbfTable.FieldValueByName(i, BuildNumField)
			house.StrucNum, _ = dbfTable.FieldValueByName(i, StrucNumField)
			house.HouseGuid, _ = dbfTable.FieldValueByName(i, HouseGuidField)
			house.HouseId, _ = dbfTable.FieldValueByName(i, HouseIdField)
			house.HouseNum, _ = dbfTable.FieldValueByName(i, HouseNumField)
			house.PostalCode, _ = dbfTable.FieldValueByName(i, PostalCodeField)
			ch <- &house
		}
	}(ch)
	return ch
}
