package repository

import (
	"database/sql"
	"fias/model"
	"fmt"
	"log"
	"strings"
)

type StreetRepository interface {
	// Save streets returns map (key is street guid, value is street id)
	SaveBatch(streets []*model.Street) map[string]int
}

type DefaultStreetRepository struct {
	db *sql.DB
}

func (repo DefaultStreetRepository) SaveBatch(streets []*model.Street) map[string]int {
	args := make([]interface{}, 0, 5*len(streets))
	placeholders := make([]string, 0, len(streets))
	for idx, street := range streets {
		varNum := idx*5 + 1
		placeholder := fmt.Sprintf("($%d, $%d, $%d, $%d, $%d)",
			varNum, varNum+1, varNum+2, varNum+3, varNum+4)
		placeholders = append(placeholders, placeholder)
		args = append(args, street.CityId)
		args = append(args, street.AoGuid)
		args = append(args, street.AoId)
		args = append(args, street.Name)
		args = append(args, street.Address)
	}
	query := `INSERT INTO street(city_id, aoguid, aoid, name, address) VALUES %s 
			  ON CONFLICT (aoguid) DO UPDATE SET city_id = EXCLUDED.city_id, aoid = EXCLUDED.aoid, name = EXCLUDED.name, address = EXCLUDED.address 
			  RETURNING id, aoguid`
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
func NewStreetRepository(db *sql.DB) StreetRepository {
	return DefaultStreetRepository{db: db}
}
