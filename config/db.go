package config

import "fmt"

const (
	defaultDb     = "postgres"
	defaultDbUser = "postgres"
	defaultDbPwd  = "postgres"
	defaultDbHost = "db"
	defaultDbPort = "5432"

	envDbName = "DB_NAME"
	envDbUser = "DB_USER"
	envDbPwd  = "DB_PWD"
	envDbHost = "DB_HOST"
	envDbPort = "DB_PORT"
)

type PgDbConnection struct {
	Name string
	User string
	Pwd  string
	Host string
	Port string
}

func (conn PgDbConnection) ConnectionStr() string {
	return fmt.Sprintf("postgres://%s:%s@%s/%s?port=%s&sslmode=%s", conn.User, conn.Pwd, conn.Host, conn.Name, conn.Port, "disable")
}

func (conn PgDbConnection) DriverName() string {
	return "postgres"
}

func NewDbConnection() *PgDbConnection {
	return &PgDbConnection{
		Name: getEnv(envDbName, defaultDb),
		User: getEnv(envDbUser, defaultDbUser),
		Pwd:  getEnv(envDbPwd, defaultDbPwd),
		Host: getEnv(envDbHost, defaultDbHost),
		Port: getEnv(envDbPort, defaultDbPort),
	}
}
