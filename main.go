package main

import (
	"fmt"
	"path"

	"github.com/recallsong/cliframe/cobrax"
	"github.com/recallsong/mysql-sync/cmd"
	"github.com/spf13/cobra"
)

func main() {
	cobrax.Execute("mysql-sync", &cobrax.Options{
		CfgDir:      path.Join(".", "conf"),
		CfgFileName: "mysql-sync",
		Init: func(rootCmd *cobra.Command) {
			cmd.InitCommand(rootCmd)
		},
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Println("nothing to do.")
		},
	})
}
