package dbf

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestReadActualAddrObjects(t *testing.T) {
	result, err := ReadActualAddrObjects("../test_data/addrobj.dbf")
	assert.NoError(t, err, "error should be nil")
	assert.Len(t, result, 1, "slice should contain 1 element")
	expectedRecord := AddrObject{
		AoId:       "23e710dd-6727-4dc1-ad50-eafb3f4634a4",
		AoGuid:     "1781f74e-be4a-4697-9c6b-493057c94818",
		AoLevel:    1,
		FormalName: "Кабардино-Балкарская",
		RegionCode: 7,
		OffName:    "Кабардино-Балкарская",
		PostalCode: "",
		TypeName:   "Респ",
		ParentGuid: "",
	}
	assert.Len(t, result, 1, "result map length should be 1")
	assert.Equal(t, &expectedRecord, result[expectedRecord.AoGuid])
}

func TestReadActualAddrObjectsFileNotExists(t *testing.T) {
	result, err := ReadActualAddrObjects("../test_data/wrong.dbf")
	assert.Nil(t, result, "dbf read result should be nil")
	assert.Error(t, err, "error should't be nil")
}
