package cmd

import (
	"github.com/lesomnus/xli"
	"github.com/lesomnus/xli/flg"
)

func NewCmdRoot() *xli.Command {
	return &xli.Command{
		Name:  "sir",
		Brief: "read SIR file",
		Flags: flg.Flags{},

		Commands: xli.Commands{
			NewCmdInspect(),
			NewCmdPrint(),
		},
		Handler: xli.Chain(
			xli.RequireSubcommand(),
		),
	}
}
