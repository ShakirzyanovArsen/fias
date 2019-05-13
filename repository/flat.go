package repository

import (
	"database/sql"
	"fias/model"
	"github.com/lib/pq"
	"log"
)

type FlatRepository interface {
	SaveBatch(flats []*model.Flat)
}

type DefaultFlatRepository struct {
	db *sql.DB
}

//func (repo DefaultFlatRepository) SaveBatch(flats []*model.Flat) {
//	if len(flats) == 0 {
//		return
//	}
//	fieldCount := 4
//	args := make([]interface{}, 0, fieldCount * len(flats))
//	placeholders := make([]string, 0, len(flats))
//	for idx, flat := range flats {
//		varNum := idx * fieldCount + 1
//		placeholder := fmt.Sprintf("($%d, $%d, $%d, $%d)",
//			varNum, varNum + 1, varNum + 2, varNum + 3)
//		placeholders = append(placeholders, placeholder)
//		args = append(args, flat.HouseId)
//		args = append(args, flat.Guid)
//		args = append(args, flat.Number)
//		args = append(args, flat.Type)
//	}
//	//query := `INSERT INTO flat(house_id, guid, number, type) VALUES %s
//	//		  ON CONFLICT (guid) DO UPDATE SET house_id = EXCLUDED.house_id, number = EXCLUDED.number,
//    //          type = EXCLUDED.type;`
//	query := `INSERT INTO flat(house_id, guid, number, type) VALUES %s ;`
//	query = fmt.Sprintf(query, strings.Join(placeholders, ","))
//	rows, err := repo.db.Query(query, args...)
//	if err != nil {
//		log.Printf()f("while insert flats error occured: %s, args: %v", err, args)
//	}
//	defer rows.Close()
//}

func (repo DefaultFlatRepository) SaveBatch(flats []*model.Flat) {
	if len(flats) == 0 {
		return
	}

	txn, err := repo.db.Begin()
	if err != nil {
		log.Printf("can't load flats into db, error occured: %s", err)
	}

	stmt, err := txn.Prepare(pq.CopyIn("flat", "house_id", "guid", "number", "type"))
	if err != nil {
		log.Printf("can't load flats into db, error occured: %s", err)
	}

	for _, flat := range flats {
		_, err = stmt.Exec(flat.HouseId, flat.Guid, flat.Number, flat.Type)
		if err != nil {
			log.Printf("can't load flats into db, error occured: %s", err)
		}
	}

	_, err = stmt.Exec()
	if err != nil {
		log.Printf("can't load flats into db, error occured: %s", err)
	}

	err = stmt.Close()
	if err != nil {
		log.Printf("can't load flats into db, error occured: %s", err)
	}

	err = txn.Commit()
	if err != nil {
		log.Printf("can't load flats into db, error occured: %s", err)
	}
}

func NewFlatRepository(db *sql.DB) FlatRepository {
	return DefaultFlatRepository{db: db}
}
