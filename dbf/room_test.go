package dbf

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestReadRooms(t *testing.T) {
	houses := map[string]int64{"7bb63be1-1500-42ca-ad3e-18f0ef4eb908": 1}
	ch := ReadRooms("../test_data/room.dbf", houses)
	expectedRoom := Room{
		RoomGuid:   "4d9ad208-6576-4ee3-96ce-0263465c095f",
		HouseGuid:  "7bb63be1-1500-42ca-ad3e-18f0ef4eb908",
		FlatNumber: "1",
		FlatType:   1,
	}
	for rooms := range ch {
		assert.Len(t, rooms, 1)
		assert.Equal(t, []*Room{&expectedRoom}, rooms)
	}

}

func TestReadFlatTypesMapping(t *testing.T) {
	flatTypesMapping, e := ReadFlatTypesMapping("../test_data/roomtype.dbf")
	assert.NoError(t, e)
	assert.Equal(t, "Не определено", flatTypesMapping[0])
	assert.Equal(t, "Комната", flatTypesMapping[1])
	assert.Equal(t, "Помещение", flatTypesMapping[2])
}

func TestReadRoomsFileNotExists(t *testing.T) {
	result, err := ReadActualAddrObjects("../test_data/wrong.dbf")
	assert.Nil(t, result)
	assert.Error(t, err)
}
