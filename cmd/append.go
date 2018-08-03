package cmd

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/astaxie/beego/orm"
	log "github.com/sirupsen/logrus"
)

type AppendStatus struct {
	Id            int64
	Source        string
	Table         string
	Pattern       string
	Prefix        string
	LastRow       string
	LastKeyValues string
}

func doAppend(srcDb, dstDb orm.Ormer, ptn *Pattern, srcTab string, stable *SourceTable, source string) (err error) {
	if len(stable.Keys) <= 0 {
		return fmt.Errorf("no keys for append table %s", srcTab)
	}
	if err := createStatusTable(dstDb); err != nil {
		return err
	}
	status := &AppendStatus{
		Source:  source,
		Table:   srcTab,
		Pattern: stable.Pattern,
		Prefix:  stable.Prefix,
	}
	status_sel := dstDb.QueryTable("append_status")
	cron := orm.NewCondition().And("source", status.Source).And("table", status.Table).
		And("pattern", status.Pattern).And("prefix", status.Prefix)
	err = status_sel.SetCond(cron).One(status)
	if err != nil {
		if err != orm.ErrNoRows {
			return err
		}
	}
	keyVals := make(map[string]interface{})
	if len(status.LastKeyValues) > 0 {
		err = json.Unmarshal([]byte(status.LastKeyValues), &keyVals)
		if err != nil {
			return err
		}
	}
	lastRow := make(orm.Params)
	if len(status.LastRow) > 0 {
		err = json.Unmarshal([]byte(status.LastRow), &lastRow)
		if err != nil {
			return err
		}
	}
	keyValues := make([]interface{}, 0, 10)
	keys := ""
	last := len(stable.Keys) - 1
	for i, key := range stable.Keys {
		if i >= last {
			keys = "`" + key + "`>=?"
		} else {
			keys = "`" + key + "`>=? AND "
		}
		if v, ok := keyVals[key]; ok {
			keyValues = append(keyValues, v)
		} else {
			keyValues = append(keyValues, "")
		}
	}
	var total int64
	query_sql := "SELECT COUNT(*) FROM `" + srcTab + "` WHERE " + keys
	log.Debug(query_sql, " ", keyValues)
	sel := srcDb.Raw(query_sql, keyValues...)
	err = sel.QueryRow(&total)
	if err != nil {
		return err
	}
	if total <= 0 {
		return nil
	}
	log.Infof("%d rows will be append to table %s", total, srcTab)
	table_name := ptn.Prefix + stable.Prefix + srcTab
	if ptn.AutoCreate {
		err = createTable(srcDb, dstDb, srcTab, table_name)
		if err != nil {
			return err
		}
	}
	result := make([]orm.Params, 0, 80000)
	buf := &bytes.Buffer{}
	var inserted int64
	err = dstDb.Begin()
	if err != nil {
		return err
	}
	values := make([]interface{}, 0, 10)
	for {
		keyValues = keyValues[0:0]
		for _, key := range stable.Keys {
			if v, ok := keyVals[key]; ok {
				keyValues = append(keyValues, v)
			} else {
				keyValues = append(keyValues, "")
			}
		}
		query_sql = "SELECT * FROM `" + srcTab + "` WHERE " + keys + " LIMIT 0,10000"
		log.Debug(query_sql, " ", keyValues)
		sel = srcDb.Raw(query_sql, keyValues...)
		_, err = sel.Values(&result)
		if err != nil {
			e := dstDb.Rollback()
			if e != nil {
				log.Warn("db rollback error: ", e)
			}
			return err
		}
		first := 0
		for _, row := range result {
			if cmpRow(lastRow, row) {
				first++
				continue
			} else {
				break
			}
		}
		result = result[first:]
		if len(result) <= 0 {
			break
		}
		values = values[0:0]
		n, _, err := doInsert(dstDb, result, table_name, values, buf)
		if err != nil {
			e := dstDb.Rollback()
			if e != nil {
				log.Warn("db rollback error: ", e)
			}
			return err
		}
		lastRow = result[len(result)-1]
		for _, key := range stable.Keys {
			keyVals[key] = lastRow[key]
		}
		inserted += n
	}
	if inserted > 0 {
		byts, err := json.Marshal(keyVals)
		if err != nil {
			e := dstDb.Rollback()
			if e != nil {
				log.Warn("db rollback error: ", e)
			}
			return err
		}
		status.LastKeyValues = string(byts)
		byts, err = json.Marshal(lastRow)
		if err != nil {
			e := dstDb.Rollback()
			if e != nil {
				log.Warn("db rollback error: ", e)
			}
			return err
		}
		status.LastRow = string(byts)
		if status.Id > 0 {
			_, err = dstDb.Update(status)
		} else {
			_, err = dstDb.Insert(status)
		}
		if err != nil {
			e := dstDb.Rollback()
			if e != nil {
				log.Warn("db rollback error: ", e)
			}
			return err
		}
	}
	e := dstDb.Commit()
	if e != nil {
		log.Warn("db rollback error: ", e)
		return e
	}
	log.Infof("import table %s to %s ok, tatal:%d, inserted:%d.", srcTab, table_name, total, inserted)
	return nil
}

func doInsert(dstDb orm.Ormer, result []orm.Params, table_name string, values []interface{}, buf *bytes.Buffer) (inserted int64, lastRow orm.Params, err error) {
	for _, row := range result {
		buf.Reset()
		buf.WriteString("INSERT INTO `")
		buf.WriteString(table_name)
		buf.WriteString("`(")
		last := len(row) - 1
		idx := 0
		values = values[0:0]
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
			return inserted, lastRow, err
		}
		inserted++
		lastRow = row
	}
	return inserted, lastRow, nil
}

func cmpRow(last, row orm.Params) bool {
	return reflect.DeepEqual(last, row)
}

func createStatusTable(dstDb orm.Ormer) (err error) {
	sql := "CREATE TABLE IF NOT EXISTS `append_status` ( \n" +
		" `id` int(11) NOT NULL AUTO_INCREMENT, \n" +
		" `source` varchar(128) NOT NULL,\n" +
		" `table` varchar(128) NOT NULL,\n" +
		" `pattern` varchar(128) NOT NULL,\n" +
		" `prefix` varchar(128) NOT NULL,\n" +
		" `last_row` varchar(1024) NOT NULL,\n" +
		" `last_key_values` varchar(1024) NOT NULL,\n" +
		" PRIMARY KEY (`id`)\n" +
		") ENGINE=InnoDB AUTO_INCREMENT=19 DEFAULT CHARSET=utf8;"
	_, err = dstDb.Raw(sql).Exec()
	return err
}
