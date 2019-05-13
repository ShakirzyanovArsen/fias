package dbf

import (
	"errors"
	"fmt"
	"github.com/LindsayBradford/go-dbf/godbf"
	"log"
	"math"
	"strconv"
)

type Room struct {
	RoomGuid   string
	HouseGuid  string
	FlatNumber string
	FlatType   int
}

const (
	RoomGuidField   = "ROOMGUID"
	FlatNumberField = "FLATNUMBER"
	FlatTypeField   = "FLATTYPE"
	RoomTypeIdField = "RMTYPEID"
	NameField       = "NAME"
)

// Reads rooms from dbf file. houses is map of houses guids to ids.
// This function reads only actual rooms (live status is 1), which house guid exists in guids map
func ReadRooms(filePath string, houses map[string]int64) <-chan *Room {
	dbfTable, err := godbf.NewFromFile(filePath, "866")
	if err != nil || dbfTable == nil {
		log.Printf("error while reading dbf file %s\n", filePath)
		return nil
	}
	ch := make(chan *Room, 10000)
	go func(ch chan *Room) {
		defer close(ch)
		for i := 0; i < dbfTable.NumberOfRecords(); i++ {
			room := Room{}
			houseGuid, err := dbfTable.FieldValueByName(i, HouseGuidField)
			if err != nil {
				log.Printf("while fetch row error occured(row skipped): %s", err)
				continue
			}
			if _, exists := houses[houseGuid]; !exists {
				continue
			}
			status, _ := dbfTable.FieldValueByName(i, LiveStatusField)
			if status != "1" {
				continue
			}
			room.RoomGuid, _ = dbfTable.FieldValueByName(i, RoomGuidField)
			room.HouseGuid = houseGuid
			room.FlatNumber, _ = dbfTable.FieldValueByName(i, FlatNumberField)
			flatTypeStr, _ := dbfTable.FieldValueByName(i, FlatTypeField)
			room.FlatType, _ = strconv.Atoi(flatTypeStr)
			ch <- &room
		}
	}(ch)
	return ch
}

// Reads flat type dictionary to map
func ReadFlatTypesMapping(filePath string) (map[int]string, error) {
	dbfTable, err := godbf.NewFromFile(filePath, "866")
	if err != nil || dbfTable == nil {
		return nil, errors.New(fmt.Sprintf("error while reading dbf file %s", filePath))
	}
	var result = make(map[int]string)
	for i := 0; i < dbfTable.NumberOfRecords(); i++ {
		idStr, _ := dbfTable.FieldValueByName(i, RoomTypeIdField)
		idFloat, err := strconv.ParseFloat(idStr, 64)
		id := int(math.Round(idFloat))
		if err != nil {
			fmt.Printf("cannot read flat type record: %s", err)
			continue
		}
		result[id], _ = dbfTable.FieldValueByName(i, NameField)
	}
	return result, nil
}
