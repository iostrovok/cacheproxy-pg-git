package pggit

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/pkg/errors"

	"github.com/iostrovok/cacheproxy/plugins"
	pgPlug "github.com/iostrovok/cacheproxy/plugins/pg"
)

const (
	fileNameCol = "file_name"
	keyCol      = "key"
	versionCol  = "version"
	dataCol     = "data"

	CreateTableSql = `
CREATE TABLE IF NOT EXISTS %s
(
    id serial,
    ` + fileNameCol + ` character varying(40) COLLATE pg_catalog."default" NOT NULL,
    ` + keyCol + ` character varying(40) COLLATE pg_catalog."default" NOT NULL,
    ` + versionCol + ` character varying(500) COLLATE pg_catalog."default" NOT NULL,
    ` + dataCol + ` bytea,
    date_create timestamp without time zone NOT NULL DEFAULT now(),
    CONSTRAINT %s_pkey PRIMARY KEY (id),
    CONSTRAINT %s_uxk UNIQUE (file_name, key, version)
        WITH (FILLFACTOR=100)
)
`

	DeleteBranchKeySql = `DELETE FROM %s WHERE ` + versionCol + ` = $1 AND ` + keyCol + ` = $2`
	DeleteBranchSql    = `DELETE FROM %s WHERE ` + versionCol + ` = $1`
	UpdateBranchSql    = `UPDATE %s SET ` + versionCol + ` = $1 WHERE ` + versionCol + ` = $2`
	CopyBranchSql      = `
	INSERT INTO %s (` + fileNameCol + `, ` + keyCol + `, ` + dataCol + `, ` + versionCol + `)
	(SELECT ` + fileNameCol + `, ` + keyCol + `, ` + dataCol + `, $1 As ` + versionCol + ` FROM %s WHERE ` + versionCol + ` = $2)
`
)

type PgGit struct {
	plugin   plugins.IPlugin
	db       *sql.DB
	branch   string // current branch
	table    string // schema + table name
	useCache bool
}

func New(db *sql.DB, branch, table string, useCache bool) (*PgGit, error) {
	out := &PgGit{
		db:       db,
		table:    table,
		branch:   branch,
		useCache: useCache,
	}

	err := out.init()

	return out, err
}

func (pgt *PgGit) init() error {
	// remove schema from name for indexes
	tableName := strings.Replace(pgt.table, ".", "", -1)
	tmp := fmt.Sprintf(CreateTableSql, pgt.table, tableName, tableName)
	_, err := pgt.db.Exec(tmp)
	if err != nil {
		return err
	}

	config := &pgPlug.Config{
		Table:      pgt.table,
		FileCol:    fileNameCol,
		KeyCol:     keyCol,
		ValCol:     dataCol,
		VersionCol: versionCol,
		UseCache:   pgt.useCache,
		UsePreload: pgt.useCache,
		Version:    pgt.branch,
	}

	pgt.plugin, err = pgPlug.New(context.Background(), pgt.db, config)
	return err
}

func (pgt *PgGit) Config() *pgPlug.Config {
	return &pgPlug.Config{
		Table:      pgt.table,
		FileCol:    fileNameCol,
		KeyCol:     keyCol,
		ValCol:     dataCol,
		VersionCol: versionCol,
		UseCache:   pgt.useCache,
		UsePreload: pgt.useCache,
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

// PreloadByVersion loads data by 1 request
func (pgt *PgGit) PreloadByVersion() error {
	return pgt.plugin.PreloadByVersion()
}

// MergeToBranch moves all current branch files to incoming branch.
func (pgt *PgGit) MergeToBranch(ctx context.Context, branch string) error {
	tmpDel := fmt.Sprintf(DeleteBranchSql, pgt.table)
	tmpUp := fmt.Sprintf(UpdateBranchSql, pgt.table)

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

// DeleteBranch just cleans brunch's files
func (pgt *PgGit) DeleteBranch(ctx context.Context, branch string) error {
	_, err := pgt.db.ExecContext(ctx, fmt.Sprintf(DeleteBranchSql, pgt.table), branch)
	return err
}

// DeleteBranchKey just removes one key from brunch's files
func (pgt *PgGit) DeleteBranchKey(ctx context.Context, branch, key string) error {
	_, err := pgt.db.ExecContext(ctx, fmt.Sprintf(DeleteBranchKeySql, pgt.table), branch, key)
	return err
}

// ReplaceFromBranch replaces all current branch files from incoming branch.
func (pgt *PgGit) ReplaceFromBranch(ctx context.Context, branch string) error {
	tmpDel := fmt.Sprintf(DeleteBranchSql, pgt.table)
	tmpCopy := fmt.Sprintf(CopyBranchSql, pgt.table, pgt.table)

	tx, err := pgt.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return err
	}

	// delete files from current branch, if they are exists
	if _, err = tx.ExecContext(ctx, tmpDel, pgt.branch); err != nil {
		if rollbackErr := tx.Rollback(); rollbackErr != nil {
			err = errors.Wrap(err, err.Error())
		}
		return err
	}

	// copy files from incoming branch
	if _, err = tx.ExecContext(ctx, tmpCopy, pgt.branch, branch); err != nil {
		if rollbackErr := tx.Rollback(); rollbackErr != nil {
			err = errors.Wrap(err, err.Error())
		}
		return err
	}

	return tx.Commit()
}
