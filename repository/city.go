package repository

import (
	"database/sql"
	"fias/model"
	"fmt"
	"log"
	"strings"
)

type CityRepository interface {
	// Save cities returns map (key is city guid, value is city id)
	SaveBatch(cities []*model.City) map[string]int
}

type DefaultCityRepository struct {
	db *sql.DB
}

func (repo DefaultCityRepository) SaveBatch(cities []*model.City) map[string]int {
	args := make([]interface{}, 0, 4*len(cities))
	placeholders := make([]string, 0, len(cities))
	for idx, city := range cities {
		varNum := idx*4 + 1
		placeholder := fmt.Sprintf("($%d, $%d, $%d, $%d)", varNum, varNum+1, varNum+2, varNum+3)
		placeholders = append(placeholders, placeholder)
		args = append(args, city.RegionId)
		args = append(args, city.AoGuid)
		args = append(args, city.Aoid)
		args = append(args, city.Name)
	}
	query := `INSERT INTO city(region_id, aoguid, aoid, name) VALUES %s 
				ON CONFLICT (aoguid) DO UPDATE SET name = EXCLUDED.name RETURNING id, aoguid`
	query = fmt.Sprintf(query, strings.Join(placeholders, ","))
	rows, err := repo.db.Query(query, args...)
	if err != nil {
		log.Fatalf("while insert cities error occured: %s", err)
	}
	defer rows.Close()
	guidMap := make(map[string]int)
	for rows.Next() {
		var id int
		var guid string
		rows.Scan(&id, &guid)
		guidMap[guid] = id
	}
	return guidMap
}

func NewCityRepository(db *sql.DB) CityRepository {
	return DefaultCityRepository{db: db}
}
