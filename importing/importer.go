package importing

import (
	"database/sql"
	"fias/config"
	"fias/dbf"
	"fias/model"
	"fias/repository"
	"fmt"
	_ "github.com/lib/pq"
	"log"
	"runtime"
	"sync"
	"time"
)

type Importer struct {
	cityRepo   repository.CityRepository
	regionRepo repository.RegionRepository
	streetRepo repository.StreetRepository
	houseRepo  repository.HouseRepository
	flatRepo   repository.FlatRepository
}

const maxChunkSize = 1000

func (importer Importer) Import(basePath string, code string) {
	flat := fetchFlatAddrObjs(basePath, code)
	regionObj := flat.all[flat.regionGuid]
	region := &model.Region{
		AoGuid: regionObj.AoGuid,
		AoId:   regionObj.AoId,
		Code:   regionObj.RegionCode,
		Name:   regionObj.FormalName,
		Type:   regionObj.TypeName,
	}
	importer.regionRepo.Save(region)
	citiesGuidsToIds := importer.importCities(flat, region)
	streetsGuidsToIds := importer.importStreets(flat, citiesGuidsToIds)

	runtime.GC()
	houseFilePath := fmt.Sprintf("%s/HOUSE%s.DBF", basePath, code)
	housesGuids := importer.importHousesAsync(houseFilePath, streetsGuidsToIds, flat)
	flat = nil
	runtime.GC()

	roomFilePath := fmt.Sprintf("%s/ROOM%s.DBF", basePath, code)
	roomTypes := fetchRoomTypes(basePath)
	importer.importFlatsAsync(roomFilePath, housesGuids, roomTypes)
	runtime.GC()
}

func (importer Importer) importFlatsAsync(roomFilePath string, housesGuids map[string]int64, roomTypes map[int]string) {
	fmt.Printf("Started flat loading\n")
	flatLoadStart := time.Now()
	var flatsWg sync.WaitGroup
	dbfRooms := make([]*dbf.Room, 0, maxChunkSize)
	roomsChannel := dbf.ReadRooms(roomFilePath, housesGuids)
	if roomsChannel != nil {
		for dbfRoom := range roomsChannel {
			if len(dbfRooms) < maxChunkSize {
				dbfRooms = append(dbfRooms, dbfRoom)
			} else {
				flatsWg.Add(1)
				chunk := make([]*dbf.Room, len(dbfRooms))
				copy(chunk, dbfRooms)
				go func() {
					importer.pushFlatsToDb(chunk, housesGuids, roomTypes)
					flatsWg.Done()
				}()
				dbfRooms = make([]*dbf.Room, 0, maxChunkSize)
			}
		}
		if len(dbfRooms) != 0 {
			importer.pushFlatsToDb(dbfRooms, housesGuids, roomTypes)
		}
		flatsWg.Wait()
	}
	flatLoadDiff := time.Since(flatLoadStart)
	fmt.Printf("Finished house loading %f\n", flatLoadDiff.Seconds())
}

func (importer Importer) importHousesAsync(houseFilePath string, streetsGuidsToIds map[string]int, flat *FlatAddrObjects) map[string]int64 {
	fmt.Printf("Started house loading\n")
	houseLoadStart := time.Now()

	housesGuids := make(map[string]int64)
	guidsMut := sync.Mutex{}
	dbfHouses := make([]*dbf.House, 0, maxChunkSize)
	var housesWg sync.WaitGroup
	for houseDbf := range dbf.ReadHouseChunks(houseFilePath, streetsGuidsToIds) {
		if len(dbfHouses) < maxChunkSize {
			dbfHouses = append(dbfHouses, houseDbf)
		} else {
			chunk := make([]*dbf.House, len(dbfHouses))
			copy(chunk, dbfHouses)
			dbfHouses = nil
			housesWg.Add(1)
			go func(guids map[string]int64, mut *sync.Mutex, houses []*dbf.House) {
				importedGuids := importer.pushHousesToDb(houses, streetsGuidsToIds, flat)
				defer housesWg.Done()
				for k, v := range importedGuids {
					mut.Lock()
					guids[k] = v
					mut.Unlock()
				}
			}(housesGuids, &guidsMut, chunk)

		}
	}
	if len(dbfHouses) != 0 {
		guids := importer.pushHousesToDb(dbfHouses, streetsGuidsToIds, flat)
		for k, v := range guids {
			guidsMut.Lock()
			housesGuids[k] = v
			guidsMut.Unlock()
		}
	}
	housesWg.Wait()
	houseLoadDiff := time.Since(houseLoadStart)
	fmt.Printf("Finished house loading %f\n", houseLoadDiff.Seconds())
	return housesGuids
}

func (importer Importer) importCities(flat *FlatAddrObjects, region *model.Region) map[string]int {
	var cities []*model.City
	for guid := range flat.citiesGuids {
		obj := flat.all[guid]
		city := &model.City{RegionId: region.Id, AoGuid: guid, Aoid: obj.AoId, Name: obj.FormalName}
		cities = append(cities, city)
	}
	citiesGuidsToIds := importer.cityRepo.SaveBatch(cities)
	return citiesGuidsToIds
}

func (importer Importer) importStreets(flat *FlatAddrObjects, citiesGuids map[string]int) map[string]int {
	var streets []*model.Street
	for guid, cityInfo := range flat.streetsGuids {
		obj := flat.all[guid]
		street := &model.Street{
			CityId:  citiesGuids[cityInfo.cityGuid],
			AoGuid:  guid,
			AoId:    obj.AoId,
			Name:    obj.FormalName,
			Address: cityInfo.address,
		}
		streets = append(streets, street)
	}
	chunkSize := 10000
	streetsGuidsToIds := make(map[string]int)
	for i := 0; i*chunkSize < len(streets); i++ {
		firstInd := i * chunkSize
		lastInd := i*chunkSize + chunkSize
		if lastInd > len(streets) {
			lastInd = len(streets) - 1
		}
		chunkStreetsGuids := importer.streetRepo.SaveBatch(streets[firstInd:lastInd])
		for k, v := range chunkStreetsGuids {
			streetsGuidsToIds[k] = v
		}
	}

	return streetsGuidsToIds
}

func (importer Importer) pushHousesToDb(dbfHouses []*dbf.House, streetsGuids map[string]int,
	flat *FlatAddrObjects) map[string]int64 {

	var houses []*model.House
	for _, dbfHouse := range dbfHouses {
		streetGuid := dbfHouse.AoGuid
		if _, guidExists := streetsGuids[streetGuid]; !guidExists {
			continue
		}
		streetAddress := flat.streetsGuids[streetGuid].address
		address := fmt.Sprintf("%s, дом %s", streetAddress, dbfHouse.HouseNum)
		if dbfHouse.StrucNum != "" {
			address = fmt.Sprintf("%s, корпус %s", address, dbfHouse.StrucNum)
		}
		if dbfHouse.BuildNum != "" {
			address = fmt.Sprintf("%s, строение %s", address, dbfHouse.BuildNum)
		}
		house := &model.House{
			StreetId:   streetsGuids[streetGuid],
			Guid:       dbfHouse.HouseGuid,
			Address:    address,
			PostalCode: dbfHouse.PostalCode,
			Number:     dbfHouse.HouseNum,
			BuildNum:   dbfHouse.BuildNum,
			StrucNum:   dbfHouse.StrucNum,
		}
		houses = append(houses, house)
	}
	chunkHousesGuids := importer.houseRepo.SaveBatch(houses)
	return chunkHousesGuids
}

func (importer Importer) pushFlatsToDb(rooms []*dbf.Room, housesGuids map[string]int64,
	flatTypes map[int]string) {

	var flats []*model.Flat
	for _, room := range rooms {
		houseGuid := room.HouseGuid
		houseId, houseExists := housesGuids[houseGuid]
		if !houseExists {
			continue
		}
		flat := &model.Flat{
			HouseId: houseId,
			Guid:    room.RoomGuid,
			Number:  room.FlatNumber,
			Type:    flatTypes[room.FlatType],
		}
		flats = append(flats, flat)
	}
	importer.flatRepo.SaveBatch(flats)
}

func configDbPool() *sql.DB {
	conf := config.NewDbConnection()
	db, err := sql.Open(conf.DriverName(), conf.ConnectionStr())
	if err != nil {
		log.Fatal(err)
	}
	if err = db.Ping(); err != nil {
		log.Fatal(err)
	}
	db.SetMaxOpenConns(20)
	db.SetMaxIdleConns(20)
	db.SetConnMaxLifetime(time.Hour)
	return db
}

func NewImporter() *Importer {
	db := configDbPool()
	regionRepository := repository.NewRegionRepository(db)
	cityRepository := repository.NewCityRepository(db)
	streetRepository := repository.NewStreetRepository(db)
	houseRepository := repository.NewHouseRepository(db)
	flatRepository := repository.NewFlatRepository(db)
	return &Importer{
		regionRepo: regionRepository,
		cityRepo:   cityRepository,
		streetRepo: streetRepository,
		houseRepo:  houseRepository,
		flatRepo:   flatRepository,
	}
}
