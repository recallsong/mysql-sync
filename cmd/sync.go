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

/*
func doSync(srcDb, dstDb orm.Ormer, ptn *Pattern, stable_n string, stable *SourceTable) (err error) {
	skip, limit := 0, 5000
	result := make([]orm.Params, 0, limit)
	colsList := make([]*Column, 0, len(ptn.Columns)+5)
	colsSet := make(map[string]*Column, len(ptn.Columns)+5)
	for _, col := range ptn.Columns {
		if _, ok := colsSet[col.Field]; ok {
			return fmt.Errorf("column %s already exist in pattern %s", col.Field, stable.Pattern)
		}
		colsSet[col.Field] = col
		colsList = append(colsList, col)
	}
	if ptn.AllColumns {
		sel := srcDb.Raw("desc `" + stable_n + "`")
		src_cols := make([]orm.Params, len(ptn.Columns)+5)
		_, err = sel.Values(&src_cols)
		if err != nil {
			return err
		}
		for _, col := range src_cols {
			field := col["Field"].(string)
			if _, ok := colsSet[field]; !ok {
				c := &Column{
					Field: field,
					Type:  col["Type"].(string),
					Null:  col["Null"].(string),
					Key:   col["Key"].(string),
				}
				if col["Default"] == nil {
					c.Default = nil
				} else {
					str := col["Default"].(string)
					c.Default = &str
				}
				if col["Extra"] == nil {
					c.Extra = nil
				} else {
					str := col["Extra"].(string)
					c.Extra = &str
				}
				colsSet[field] = c
				colsList = append(colsList, c)
			}
		}
	}
	if len(colsList) <= 0 {
		return fmt.Errorf("no column to insert for table %s by pattern %s", stable_n, stable.Pattern)
	}
	table_name := ptn.Prefix + stable.Prefix + stable_n
	if ptn.AutoCreate {
		err = createTable(dstDb, table_name, colsList)
		if err != nil {
			return err
		}
	}
	buf := bytes.Buffer{}
	for _, col := range colsList {
		buf.WriteString("`" + col.Field + "`,")
	}
	cols := string(buf.Bytes()[0 : buf.Len()-1])
	// query_sql := "SELECT " + cols + " FROM `" + stable_n + "` LIMIT ?,?"
	query_sql := "SELECT " + cols + " FROM `" + stable_n + "`"
	sel := srcDb.Raw(query_sql, skip, limit)
	_, err = sel.Values(&result)
	if err != nil {
		return err
	}
	values := make([]interface{}, 0, len(colsList))
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
		fmt.Println(insert_sql)
		_, err = dstDb.Raw(insert_sql, values...).Exec()
		if err != nil {
			return err
		}
		values = values[0:0]
	}
	log.Infof("import table %s to %s ok, %d", stable_n, table_name, len(result))
	return nil
}
*/
