package repository

import (
	"database/sql"
	"fias/model"
	"log"
)

type RegionRepository interface {
	// Save region in db, set region id
	Save(region *model.Region)
}

type DefaultRegionRepository struct {
	db *sql.DB
}

func (repo DefaultRegionRepository) Save(region *model.Region) {
	query := `INSERT INTO region (code, name, aoguid, aoid, "type") VALUES ($1, $2, $3, $4, $5) 
			  ON CONFLICT(aoguid) DO UPDATE SET name = EXCLUDED.name, aoid = EXCLUDED.aoid, type = EXCLUDED.type 
			  RETURNING id;`
	rows, err := repo.db.Query(query, region.Code, region.Name, region.AoGuid, region.AoId, region.Type)
	if err != nil {
		log.Fatalf("'%s' while insert region: %v", err, region)
	}
	defer rows.Close()
	rows.Next()
	rows.Scan(&region.Id)
}

func NewRegionRepository(db *sql.DB) RegionRepository {
	return DefaultRegionRepository{db: db}
}
