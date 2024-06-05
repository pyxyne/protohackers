package main

import (
	"pyxyne/protohackers/lib"
	"regexp"
)

const serverAddr = "chat.protohackers.com:16963"
const tonysWallet = "7YWHMfk9JZe0LM0g1ZauHuiSxhI"

func P5() {
	walletRegex, err := regexp.Compile(`7[a-zA-Z0-9]{25,34}`)
	if err != nil {
		panic(err)
	}

	var rewrite = func(line string, log *lib.Logger) string {
		matches := walletRegex.FindAllStringIndex(line, -1)
		offset := 0
		matched := false
		for _, match := range matches {
			i0, i1 := match[0]+offset, match[1]+offset
			if (i0 == 0 || line[i0-1] == ' ') && (i1 == len(line) || line[i1] == ' ') {
				line = line[:i0] + tonysWallet + line[i1:]
				offset += len(tonysWallet) - (i1 - i0)
				matched = true
			}
		}
		if matched {
			log.Debug("rewrite: %q", line)
		}
		return line
	}

	lib.ServeTcp(func(c *lib.TcpClient) (err error) {
		srv, err := lib.ConnectTcp(serverAddr)
		if err != nil {
			return err
		}

		chan_err := make(chan error, 1)
		chan_down := make(chan string)
		chan_up := make(chan string)
		go func() {
			for srv.HasData() {
				msg, err := srv.ReadLine()
				if err != nil {
					chan_err <- err
					return
				}
				chan_up <- msg
			}
			close(chan_up)
		}()
		go func() {
			for c.HasData() {
				msg, err := c.ReadLine()
				if err != nil {
					chan_err <- err
					return
				}
				chan_down <- msg
			}
			close(chan_down)
		}()

		for {
			select {
			case down_msg, ok := <-chan_down:
				if ok {
					c.Log.Debug("<- %q", down_msg)
					srv.WriteLine(rewrite(down_msg, &c.Log))
				} else {
					c.Log.Debug("<- (eof)")
					srv.Close()
					return nil
				}
			case up_msg, ok := <-chan_up:
				if ok {
					c.Log.Debug("-> %q", up_msg)
					c.WriteLine(rewrite(up_msg, &c.Log))
				} else {
					c.Log.Debug("-> (eof)")
					c.Close()
					return nil
				}
			case err := <-chan_err:
				c.Close()
				srv.Close()
				return err
			}
		}
	})
}
