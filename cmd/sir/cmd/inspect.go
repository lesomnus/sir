package cmd

import (
	"context"
	"fmt"
	"os"

	"github.com/lesomnus/sir"
	"github.com/lesomnus/xli"
	"github.com/lesomnus/xli/arg"
)

func NewCmdInspect() *xli.Command {
	return &xli.Command{
		Name:  "inspect",
		Brief: "inspect SIR file",

		Args: arg.Args{
			&arg.String{
				Name: "file",
			},
		},

		Handler: xli.OnRun(func(ctx context.Context, cmd *xli.Command, next xli.Next) error {
			filename := arg.MustGet[string](cmd, "file")

			f, err := os.Open(filename)
			if err != nil {
				return fmt.Errorf("open: %w", err)
			}

			h, err := sir.ReadHeader(f)
			if err != nil {
				return fmt.Errorf("read header: %w", err)
			}

			cmd.Printf("   Compression: %s\n", h.Compression.String())
			cmd.Printf("Content Length: %d\n", h.ContentLength)
			cmd.Printf("Index Table At: %d\n", h.IndexTableOffset)
			cmd.Printf("First Block At: %d\n", h.FirstBlockOffset)

			return next(ctx)
		}),
	}
}
