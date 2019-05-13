package repository

import (
	"database/sql"
	"fias/model"
	"fmt"
	"log"
	"strings"
)

type HouseRepository interface {
	// Save houses returns map (key is house guid, value is house id)
	SaveBatch(houses []*model.House) map[string]int64
}

type DefaultHouseRepository struct {
	db *sql.DB
}

func (repo DefaultHouseRepository) SaveBatch(houses []*model.House) map[string]int64 {
	fieldCount := 7
	args := make([]interface{}, 0, fieldCount*len(houses))
	placeholders := make([]string, 0, len(houses))
	for idx, house := range houses {
		varNum := idx*fieldCount + 1
		placeholder := fmt.Sprintf("($%d, $%d, $%d, $%d, $%d, $%d, $%d)",
			varNum, varNum+1, varNum+2, varNum+3, varNum+4, varNum+5, varNum+6)
		placeholders = append(placeholders, placeholder)
		args = append(args, house.StreetId)
		args = append(args, house.Guid)
		args = append(args, house.Address)
		args = append(args, house.PostalCode)
		args = append(args, house.Number)
		args = append(args, house.BuildNum)
		args = append(args, house.StrucNum)
	}
	query := `INSERT INTO house(street_id, guid, address, postal_code, number, build_num, struc_num) VALUES %s 
			  ON CONFLICT (guid) DO UPDATE SET street_id = EXCLUDED.street_id, address = EXCLUDED.address,
			  postal_code = EXCLUDED.postal_code, number = EXCLUDED.number, build_num = EXCLUDED.build_num, 
             struc_num = EXCLUDED.struc_num RETURNING id, guid`
	//query := `INSERT INTO house(street_id, guid, address, postal_code, number, build_num, struc_num) VALUES %s
	//		  RETURNING id, guid`
	query = fmt.Sprintf(query, strings.Join(placeholders, ","))
	rows, err := repo.db.Query(query, args...)
	if err != nil {
		log.Fatalf("while insert houses error occured: %s", err)
	}
	defer rows.Close()
	guidMap := make(map[string]int64)
	for rows.Next() {
		var id int64
		var guid string
		rows.Scan(&id, &guid)
		guidMap[guid] = id
	}
	return guidMap
}
func NewHouseRepository(db *sql.DB) HouseRepository {
	return DefaultHouseRepository{db: db}
}
