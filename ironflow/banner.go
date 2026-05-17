package ironflow

import (
	"fmt"
	"sync"
)

// Banner is the ASCII art logo for IronFlow.
const Banner = `
 ██╗██████╗  ██████╗ ███╗   ██╗███████╗██╗      ██████╗ ██╗    ██╗
 ██║██╔══██╗██╔═══██╗████╗  ██║██╔════╝██║     ██╔═══██╗██║    ██║
 ██║██████╔╝██║   ██║██╔██╗ ██║█████╗  ██║     ██║   ██║██║ █╗ ██║
 ██║██╔══██╗██║   ██║██║╚██╗██║██╔══╝  ██║     ██║   ██║██║███╗██║
 ██║██║  ██║╚██████╔╝██║ ╚████║██║     ███████╗╚██████╔╝╚███╔███╔╝
 ╚═╝╚═╝  ╚═╝ ╚═════╝ ╚═╝  ╚═══╝╚═╝     ╚══════╝ ╚═════╝  ╚══╝╚══╝
`

var bannerOnce sync.Once

// PrintBanner prints the IronFlow ASCII banner to stdout.
// It is safe to call multiple times; the banner only prints once per process.
func PrintBanner() {
	bannerOnce.Do(func() {
		fmt.Print(Banner)
	})
}
