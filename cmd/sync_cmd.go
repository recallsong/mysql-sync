package cmd

import (
	"bytes"
	"fmt"

	"github.com/astaxie/beego/orm"
	_ "github.com/go-sql-driver/mysql"
	"github.com/recallsong/cliframe/cobrax"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

func init() {
	orm.RegisterDriver("mysql", orm.DRMySQL)
}

func InitCommand(rootCmd *cobra.Command) {
	rootCmd.AddCommand(syncCmd)
}

var syncCfg map[string]interface{}
var syncCmd = &cobra.Command{
	Use: "sync",
	Run: func(cmd *cobra.Command, args []string) {
		cobrax.InitCommand(&syncCfg)
		targets := readTatgets()
		sources := readSources()
		if err := syncAll(targets, sources); err != nil {
			log.Fatal("[sync] ", err)
			return
		}
		log.Info("[sync] ", "successful")
	},
}

func syncAll(targets map[string]*Target, sources map[string]*Source) (err error) {
	err = registerDataBase(targets, sources)
	if err != nil {
		return err
	}
	// 开始导入数据
	srcDb := orm.NewOrm()
	dstDb := orm.NewOrm()
	for sname, src := range sources {
		dname := src.Target.Name
		dst, ok := targets[dname]
		if !ok {
			return fmt.Errorf("target %s not exist for srouce %s", sname, dname)
		}
		srcDb.Using("src_" + sname)
		dstDb.Using("dst_" + dname)
		for _, table := range src.Target.Tables {
			ptn, ok := dst.Patterns[table.Pattern]
			if !ok {
				return fmt.Errorf("pattern %s not exist for table %s", table.Pattern, table.Table)
			}
			if ptn.Strategy == "sync" {
				if err := doSync(srcDb, dstDb, ptn, table.Table, table); err != nil {
					return err
				}
			} else if ptn.Strategy == "append" {
				if err := doAppend(srcDb, dstDb, ptn, table.Table, table, sname); err != nil {
					return err
				}
			} else {
				return fmt.Errorf("invalid strategy %s", ptn.Strategy)
			}
		}
	}
	return nil
}

func registerDataBase(targets map[string]*Target, sources map[string]*Source) error {
	for name, src := range sources {
		orm.RegisterDataBase("src_"+name, "mysql", src.Url)
	}
	default_db := ""
	for name, src := range targets {
		if len(default_db) <= 0 {
			default_db = src.Url
		}
		orm.RegisterDataBase("dst_"+name, "mysql", src.Url)
	}
	if len(targets) <= 0 || len(default_db) <= 0 {
		return fmt.Errorf("no target database or no default target")
	}
	orm.RegisterDataBase("default", "mysql", default_db)
	orm.RegisterModel(new(AppendStatus))
	return nil
}

func createTable(srcDb, dstDb orm.Ormer, srcTab, dstTab string) error {
	cols, err := queryColumns(srcDb, dstDb, srcTab)
	if err != nil {
		return err
	}
	if len(cols) <= 0 {
		return fmt.Errorf("no column in table %s", srcTab)
	}
	buf := bytes.Buffer{}
	buf.WriteString("CREATE TABLE IF NOT EXISTS `")
	buf.WriteString(dstTab)
	buf.WriteString("` (\n")
	keys := ""
	last := len(cols) - 1
	for i, col := range cols {
		buf.WriteString(" `" + col.Field + "` " + col.Type)
		if col.Key == "PRI" {
			keys += "`" + col.Field + "`,"
			buf.WriteString(" NOT NULL")
		} else if col.Null == "NO" {
			buf.WriteString(" NOT NULL")
		}
		if col.Default != nil {
			buf.WriteString(" DEFAULT " + *col.Default)
		}
		if i < last {
			buf.WriteString(",\n")
		} else {
			if keys != "" {
				buf.WriteString(", \nPRIMARY KEY (" + keys[0:len(keys)-1] + ")\n")
			} else {
				buf.WriteString("\n")
			}
			break
		}
	}
	buf.WriteString(") ENGINE=InnoDB DEFAULT CHARSET=utf8;")
	sql := buf.String()
	log.Debug(sql)
	_, err = dstDb.Raw(sql).Exec()
	return err
}

func queryColumns(srcDb, dstDb orm.Ormer, srcTab string) ([]*Column, error) {
	colsList := make([]*Column, 0, 10)
	sel := srcDb.Raw("desc `" + srcTab + "`")
	src_cols := make([]orm.Params, 0, 10)
	_, err := sel.Values(&src_cols)
	if err != nil {
		return nil, err
	}
	for _, col := range src_cols {
		c := &Column{
			Field: col["Field"].(string),
			Type:  col["Type"].(string),
			Null:  col["Null"].(string),
			Key:   col["Key"].(string),
		}
		if col["Default"] == nil {
			c.Default = nil
		} else {
			str := fmt.Sprint(col["Default"])
			c.Default = &str
		}
		if col["Extra"] == nil {
			c.Extra = nil
		} else {
			str := fmt.Sprint(col["Extra"])
			c.Extra = &str
		}
		colsList = append(colsList, c)
	}
	return colsList, nil
}
