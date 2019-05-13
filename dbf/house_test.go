package dbf

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestReadHouses(t *testing.T) {
	streets := map[string]int{"7f4d6291-021b-4f41-886e-27d09e1029ae": 1}
	ch := ReadHouseChunks("../test_data/house.dbf", streets)
	for houses := range ch {
		assert.Len(t, houses, 1)
		expectedHouse := &House{
			AoGuid:     "7f4d6291-021b-4f41-886e-27d09e1029ae",
			BuildNum:   "2",
			StrucNum:   "3",
			HouseGuid:  "f0362ceb-0f0c-483c-9f36-6df41739303b",
			HouseId:    "7c4292a5-4641-4de0-a8d1-01804d1a4d6f",
			HouseNum:   "9",
			PostalCode: "689125",
		}
		assert.Equal(t, []*House{expectedHouse}, houses)
	}
}
