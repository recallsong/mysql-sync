package cmd

import (
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/recallsong/cliframe/cobrax"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

type Target struct {
	Url      string
	Patterns map[string]*Pattern
}
type Pattern struct {
	Strategy   string
	Prefix     string
	AutoCreate bool
	// AllColumns bool
	// Columns    []*Column
}

type Column struct {
	Field   string
	Type    string
	Null    string
	Key     string
	Default *string
	Extra   *string
}

func readTatgets() map[string]*Target {
	targets := map[string]*Target{}
	err := filepath.Walk(path.Join(cobrax.CfgDir, "targets"), func(path string, f os.FileInfo, err error) error {
		if f == nil {
			return err
		}
		if f.IsDir() {
			return nil
		}
		n, t := readTatget(path)
		if t != nil {
			targets[n] = t
		}
		return nil
	})
	if err != nil {
		log.Fatal("[target] ", err)
		return nil
	}
	return targets
}

func readTatget(file string) (string, *Target) {
	viper := viper.New()
	viper.SetConfigFile(file)
	idx := strings.LastIndex(file, "/")
	name := file
	if idx >= 0 {
		name = name[idx+1:]
	}
	idx = strings.LastIndex(name, ".")
	if idx >= 0 {
		name = name[0:idx]
	}
	if len(name) <= 0 {
		return "", nil
	}
	err := viper.ReadInConfig()
	if err != nil {
		log.Fatalf("[target] read %s error : %v", file, err)
		return "", nil
	}
	t := &Target{}
	err = viper.Unmarshal(t)
	if err != nil {
		log.Fatalf("[target] invalid target %s, error : %v", name, err)
		return "", nil
	}
	log.Info("[target] read target ", name)
	return name, t
}

// func (p *Pattern) ToColumns() string {
// 	if len(p.Columns) <= 0 {
// 		return ""
// 	}
// 	buf := &bytes.Buffer{}
// 	for _, c := range p.Columns {
// 		buf.WriteString("`" + c.Field + "`,")
// 	}
// 	return string(buf.Bytes()[0 : buf.Len()-1])
// }
