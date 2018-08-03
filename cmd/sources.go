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

type Source struct {
	Url    string
	Target struct {
		Name   string
		Tables []*SourceTable
	}
}

type SourceTable struct {
	Table   string
	Pattern string
	Prefix  string
	Keys    []string
}

func readSources() map[string]*Source {
	sources := map[string]*Source{}
	err := filepath.Walk(path.Join(cobrax.CfgDir, "sources"), func(path string, f os.FileInfo, err error) error {
		if f == nil {
			return err
		}
		if f.IsDir() {
			return nil
		}
		n, s := readSource(path)
		if s != nil {
			sources[n] = s
		}
		return nil
	})
	if err != nil {
		log.Fatal("[source] ", err)
		return nil
	}
	return sources
}

func readSource(file string) (string, *Source) {
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
		log.Fatalf("[source] read %s error : %v", file, err)
		return "", nil
	}
	s := &Source{}
	err = viper.Unmarshal(s)
	if err != nil {
		log.Fatalf("[source] invalid source %s, error : %v", name, err)
		return "", nil
	}
	log.Info("[source] read source ", name)
	return name, s
}
