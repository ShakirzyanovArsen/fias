package dbf

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestReadTypeDict(t *testing.T) {
	typeDict, err := ReadAbbreviations("../test_data/socrbase.dbf")
	assert.NoError(t, err, "err should be nil")
	expected := map[string]string{"обл.": "Область", "г.": "Город"}
	assert.Equal(t, expected["обл"], typeDict["обл."])
	assert.Equal(t, expected["г."], typeDict["г."])
}
