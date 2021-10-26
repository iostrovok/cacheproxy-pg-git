package pggit

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/pkg/errors"

	"github.com/iostrovok/cacheproxy/plugins"
	pgPlug "github.com/iostrovok/cacheproxy/plugins/pg"
)

const (
	CreateTableSql = `
CREATE TABLE IF NOT EXISTS %s
(
    id serial,
    file_name character varying(40) COLLATE pg_catalog."default" NOT NULL,
    key character varying(40) COLLATE pg_catalog."default" NOT NULL,
    version character varying(500) COLLATE pg_catalog."default" NOT NULL,
    data bytea,
    date_create timestamp without time zone NOT NULL DEFAULT now(),
    CONSTRAINT dbfiles_pkey PRIMARY KEY (id),
    CONSTRAINT dbfiles_uxk UNIQUE (file_name, key, version)
        WITH (FILLFACTOR=100)
)
`

	DeleteVersionSql = `DELETE FROM %s WHERE version = $1`
	UpdateVersionSql = `UPDATE %s SET version = $1 WHERE version = $2`
)

type PgGit struct {
	plugin plugins.IPlugin
	db     *sql.DB
	branch string // current brunch
	table  string // schema + table name
}

func New(db *sql.DB, branch, table string) (*PgGit, error) {
	out := &PgGit{
		db:     db,
		table:  table,
		branch: branch,
	}

	err := out.init()

	return out, err
}

func (pgt *PgGit) init() error {
	tmp := fmt.Sprintf(CreateTableSql, pgt.table)
	_, err := pgt.db.Exec(tmp)
	if err != nil {
		return err
	}

	config := &pgPlug.Config{
		Table:      pgt.table,
		FileCol:    "file_name",
		KeyCol:     "key",
		ValCol:     "data",
		VersionCol: "version",
		UseCache:   true,
		UsePreload: true,
		Version:    pgt.branch,
	}

	pgt.plugin, err = pgPlug.New(context.Background(), pgt.db, config)
	return err
}

func (pgt *PgGit) Config() *pgPlug.Config {
	return &pgPlug.Config{
		Table:      pgt.table,
		FileCol:    "file_name",
		KeyCol:     "key",
		ValCol:     "data",
		VersionCol: "version",
		UseCache:   true,
		UsePreload: true,
		Version:    pgt.branch,
	}
}

func (pgt *PgGit) Plugin() plugins.IPlugin {
	return pgt.plugin
}

func (pgt *PgGit) Save(file, key string, data []byte) error {
	return pgt.plugin.Save(file, key, data)
}

func (pgt *PgGit) Read(file, key string) ([]byte, error) {
	return pgt.plugin.Read(file, key)
}

func (pgt *PgGit) SetVersion(branch string) error {
	pgt.branch = branch

	return pgt.plugin.SetVersion(branch)
}

func (pgt *PgGit) MergeTo(ctx context.Context, branch string) error {
	tmpDel := fmt.Sprintf(DeleteVersionSql, pgt.table)
	tmpUp := fmt.Sprintf(UpdateVersionSql, pgt.table)

	tx, err := pgt.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return err
	}

	if _, err = tx.ExecContext(ctx, tmpDel, branch); err != nil {
		if rollbackErr := tx.Rollback(); rollbackErr != nil {
			err = errors.Wrap(err, err.Error())
		}
		return err
	}

	if _, err = tx.ExecContext(ctx, tmpUp, branch, pgt.branch); err != nil {
		if rollbackErr := tx.Rollback(); rollbackErr != nil {
			err = errors.Wrap(err, err.Error())
		}
		return err
	}

	return tx.Commit()
}

func (pgt *PgGit) DeleteVersion(ctx context.Context, branch string) error {
	_, err := pgt.db.ExecContext(ctx, fmt.Sprintf(DeleteVersionSql, pgt.table), branch)
	return err
}
