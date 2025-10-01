package cmd

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"strings"

	"github.com/lesomnus/sir"
	"github.com/lesomnus/xli"
	"github.com/lesomnus/xli/arg"
	"github.com/lesomnus/xli/flg"
)

func NewCmdPrint() *xli.Command {
	return &xli.Command{
		Name:  "print",
		Brief: "print SIR file",

		Flags: flg.Flags{
			&flg.Switch{Name: "hex", Alias: 'x'},
			&flg.Int64{Name: "size"},
			&flg.Int64{Name: "each"},
		},
		Args: arg.Args{
			&arg.String{
				Name: "file",
			},
		},

		Handler: xli.OnRun(func(ctx context.Context, cmd *xli.Command, next xli.Next) error {
			const LineSize = 64
			var (
				printer = printByte

				size int64
				each int64
			)
			flg.Visit(cmd, "hex", func(v bool) {
				if v {
					printer = printHex
				}
			})
			flg.VisitP(cmd, "size", &size)
			flg.VisitP(cmd, "each", &each)

			filename := arg.MustGet[string](cmd, "file")

			if each == 0 {
				each = 5
			}
			if size == 0 {
				size = math.MaxInt64
			}

			stream, err := sir.OpenFile(func() (io.ReadSeeker, error) {
				return os.Open(filename)
			})
			if err != nil {
				return fmt.Errorf("open: %w", err)
			}

			r := stream.Reader(0)
			defer r.Close()

			// Origin value of `each`.
			each_v := each

			l := &strings.Builder{}
			for {
				data, err := r.Next()
				if err != nil {
					if errors.Is(err, io.EOF) {
						fmt.Println("EOF")
						break
					}
					return fmt.Errorf("read next: %w", err)
				}

				for _, d := range data {
					if size <= 0 {
						break
					}

					l.Reset()
					if each > 0 {
						l.WriteString("\n")
					} else {
						each = each_v
						cmd.Scanln()
					}

					bs := d
					if len(d) > LineSize/2 {
						bs = bs[:LineSize/2]
					}

					for _, b := range bs {
						l.WriteString(" ")
						l.WriteString(printer(b))
					}
					if len(bs) < LineSize/2 {
						l.WriteString(strings.Repeat("  ", LineSize/2-len(bs)))
					}
					if len(d) <= LineSize {
						bs := d[LineSize/2:]
						for _, b := range bs {
							l.WriteString(" ")
							l.WriteString(printer(b))
						}
						if len(bs) < LineSize/2 {
							l.WriteString(strings.Repeat("  ", LineSize/2-len(bs)))
						}
					} else {
						n := fmt.Sprintf("%d", len(d)-(64-20))
						pad := strings.Repeat(" ", 8-len(n))

						fmt.Fprintf(l, " %s...%s more   ", pad, n)

						bs := d[len(d)-20:]
						for _, b := range bs {
							l.WriteString(" ")
							l.WriteString(printer(b))
						}
					}

					l.WriteString(" ")
					cmd.Print(l.String())

					each--
					size--
				}
			}

			return next(ctx)
		}),
	}
}

func printByte(b byte) string {
	if '!' < b && b <= '~' {
		return string(b)
	}
	return "."
}

func printHex(b byte) string {
	return fmt.Sprintf("%02x", b)
}
