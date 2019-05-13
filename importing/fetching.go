package importing

import (
	"bytes"
	"fias/dbf"
	"fmt"
	"log"
	"strings"
)

type StreetInfo struct {
	address  string
	cityGuid string
}

type FlatAddrObjects struct {
	all           map[string]*dbf.AddrObject
	abbreviations map[string]string
	regionGuid    string
	citiesGuids   map[string]bool
	streetsGuids  map[string]*StreetInfo
}

func fetchFlatAddrObjs(basePath string, code string) *FlatAddrObjects {
	addrobPath := fmt.Sprintf("%s/ADDROB%s.DBF", basePath, code)
	flat := &FlatAddrObjects{}
	objects, e := dbf.ReadActualAddrObjects(addrobPath)
	flat.all = objects
	flat.streetsGuids = make(map[string]*StreetInfo)
	if e != nil {
		log.Fatal(e)
	}
	typeDictPath := fmt.Sprintf("%s/SOCRBASE.DBF", basePath)
	abbreviations, e := dbf.ReadAbbreviations(typeDictPath)
	if e != nil {
		log.Fatal(e)
	}
	flat.abbreviations = abbreviations
	lookupRegionGuid(flat)
	lookupCities(flat)

	for guid, obj := range flat.all {
		if obj.TypeName == "ул." || obj.TypeName == "ул" {
			lookupStreet(guid, flat)
		}
	}
	return flat
}

func fetchRoomTypes(basePath string) map[int]string {
	typesFilePath := fmt.Sprintf("%s/ROOMTYPE.DBF", basePath)
	mapping, err := dbf.ReadFlatTypesMapping(typesFilePath)
	if err != nil {
		log.Fatal(err)
	}
	return mapping
}

func lookupCities(flat *FlatAddrObjects) {
	var cities = make(map[string]bool)
	for uuid, obj := range flat.all {
		if obj.TypeName == "г." || obj.TypeName == "г" {
			cities[uuid] = true
		}
	}
	flat.citiesGuids = cities
}

func lookupRegionGuid(flat *FlatAddrObjects) {
	for uuid, v := range flat.all {
		if v.AoLevel == 1 {
			flat.regionGuid = uuid
			break
		}
	}
}

func lookupStreet(streetGuid string, flat *FlatAddrObjects) {
	finded := false
	var addressParts []string
	var currentGuid = streetGuid
	for !finded && currentGuid != "" {
		currObject := flat.all[currentGuid]
		typeNameFull := flat.abbreviations[currObject.TypeName]
		builder := bytes.Buffer{}
		builder.WriteString(strings.ToLower(typeNameFull))
		builder.WriteString(" ")
		builder.WriteString(currObject.OffName)
		addressParts = append([]string{builder.String()}, addressParts...)
		if flat.citiesGuids[currentGuid] {
			finded = true
		} else {
			currentGuid = currObject.ParentGuid
		}
	}
	if finded {
		region := flat.all[flat.regionGuid]
		if region.TypeName == "г" || region.TypeName == "г." {
			regionTypeName := flat.abbreviations[region.TypeName]
			addressParts = append([]string{region.FormalName + " " + strings.ToLower(regionTypeName)}, addressParts...)
		}
		address := strings.Join(addressParts[:], ", ")
		streetInf := StreetInfo{}
		streetInf.cityGuid = currentGuid
		streetInf.address = address
		flat.streetsGuids[streetGuid] = &streetInf
	}
}
