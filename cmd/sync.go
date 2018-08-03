package cmd

import (
	"bytes"

	"github.com/astaxie/beego/orm"
	log "github.com/sirupsen/logrus"
)

func doSync(srcDb, dstDb orm.Ormer, ptn *Pattern, srcTab string, stable *SourceTable) (err error) {
	result := make([]orm.Params, 0, 80000)
	table_name := ptn.Prefix + stable.Prefix + srcTab
	if ptn.AutoCreate {
		err = createTable(srcDb, dstDb, srcTab, table_name)
		if err != nil {
			return err
		}
	}
	query_sql := "SELECT * FROM `" + srcTab + "`"
	sel := srcDb.Raw(query_sql)
	_, err = sel.Values(&result)
	if err != nil {
		return err
	}
	err = dstDb.Begin()
	if err != nil {
		return err
	}
	sel = dstDb.Raw("TRUNCATE TABLE `" + table_name + "`")
	_, err = sel.Exec()
	if err != nil {
		e := dstDb.Rollback()
		if e != nil {
			log.Warn("db rollback error: ", e)
		}
		return err
	}
	buf := bytes.Buffer{}
	values := make([]interface{}, 0, 10)
	for _, row := range result {
		buf.Reset()
		buf.WriteString("INSERT INTO `")
		buf.WriteString(table_name)
		buf.WriteString("`(")
		last := len(row) - 1
		idx := 0
		for col, val := range row {
			buf.WriteString("`")
			buf.WriteString(col)
			if idx >= last {
				buf.WriteString("`) VALUES(")
			} else {
				buf.WriteString("`,")
			}
			values = append(values, val)
			idx++
		}
		for idx = 0; idx <= last; idx++ {
			if idx >= last {
				buf.WriteString("?)")
				break
			}
			buf.WriteString("?,")
		}
		insert_sql := buf.String()
		log.Debug(insert_sql, " ", values)
		_, err = dstDb.Raw(insert_sql, values...).Exec()
		if err != nil {
			e := dstDb.Rollback()
			if e != nil {
				log.Warn("db rollback error: ", e)
			}
			return err
		}
		values = values[0:0]
	}
	e := dstDb.Commit()
	if e != nil {
		log.Warn("db commit error: ", e)
		return e
	}
	log.Infof("import table %s to %s ok, %d", srcTab, table_name, len(result))
	return nil
}
