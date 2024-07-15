package main

import (
	"cmp"
	"fmt"
	"pyxyne/protohackers/lib"
	"slices"
	"strconv"
	"strings"
	"sync"
)

func parsePath(path string) []string {
	if path == "" || path[0] != '/' {
		return nil
	}
	if path == "/" {
		return []string{}
	}
	parts := strings.Split(strings.TrimPrefix(path, "/"), "/")
	for _, part := range parts {
		if part == "" {
			return nil
		}
		for _, c := range part {
			if !strings.ContainsRune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_.-", c) {
				return nil
			}
		}
	}
	return parts
}

func parseDirPath(path string) (string, bool) {
	if len(path) > 1 {
		path = strings.TrimSuffix(path, "/")
	}
	parts := parsePath(path)
	if parts == nil {
		return "", false
	}
	return strings.Join(parts, "/"), true
}

func parseFilePath(path string) (string, string, bool) {
	parts := parsePath(path)
	if len(parts) == 0 {
		return "", "", false
	}
	i := len(parts) - 1
	return strings.Join(parts[:i], "/"), parts[i], true
}

func P10() {
	m := sync.RWMutex{}
	dirs := make(map[string]map[string][]string) // dir path -> file name -> ref index -> data

	lib.ServeTcp(func(c *lib.TcpClient) error {
	cmdLoop:
		for {
			c.WriteLine("READY")
			if !c.HasData() {
				break
			}
			line, err := c.ReadLine()
			if err != nil {
				return err
			}

			args := strings.Fields(line)
			cmd := strings.ToLower(args[0])
			switch cmd {
			case "help":
				c.WriteLine("OK usage: HELP|GET|PUT|LIST")
			case "list":
				if len(args) != 2 {
					c.WriteLine("ERR usage: LIST dir")
					continue
				}
				path, ok := parseDirPath(args[1])
				if !ok {
					c.WriteLine("ERR illegal dir name")
					continue
				}

				entries := make(map[string]string)

				m.RLock()
				dir, ok := dirs[path]
				if ok {
					for filename, revs := range dir {
						entries[filename] = fmt.Sprintf("r%d", len(revs))
					}
				}
				var prefix string
				if len(path) > 0 {
					prefix = path + "/"
				}
				for dirPath := range dirs {
					if !strings.HasPrefix(dirPath, prefix) || dirPath == path {
						continue
					}
					dirName := dirPath[len(prefix):]
					if i := strings.Index(dirName, "/"); i != -1 {
						dirName = dirName[:i]
					}
					if _, ok := entries[dirName]; !ok {
						entries[dirName+"/"] = "DIR"
					}
				}
				m.RUnlock()

				c.WriteLine(fmt.Sprintf("OK %d", len(entries)))
				list := make([][2]string, 0, len(entries))
				for name, desc := range entries {
					list = append(list, [2]string{name, desc})
				}
				slices.SortFunc(list, func(p1 [2]string, p2 [2]string) int {
					return cmp.Compare[string](p1[0], p2[0])
				})
				for _, pair := range list {
					c.WriteLine(fmt.Sprintf("%s %s", pair[0], pair[1]))
				}

			case "put":
				if len(args) != 3 {
					c.WriteLine("ERR usage: PUT file length newline data")
					continue
				}
				dirPath, fileName, ok := parseFilePath(args[1])
				if !ok {
					c.WriteLine("ERR illegal file name")
					continue
				}
				length, err := strconv.Atoi(args[2])
				if err != nil {
					length = 0
				}
				dataBytes, err := c.ReadExactly(length)
				if err != nil {
					return err
				}
				for _, b := range dataBytes {
					if (b < 32 && !slices.Contains([]byte("\t\r\n"), b)) || b >= 127 {
						c.WriteLine("ERR non-text character in file")
						continue cmdLoop
					}
				}
				data := string(dataBytes)

				c.Log.Debug("Inserting file")

				m.Lock()
				if _, ok := dirs[dirPath]; !ok {
					dirs[dirPath] = make(map[string][]string)
				}
				dir := dirs[dirPath]
				if _, ok := dir[fileName]; !ok {
					dir[fileName] = make([]string, 0, 1)
				}
				refs := dir[fileName]
				if len(refs) == 0 || refs[len(refs)-1] != data {
					refs = append(refs, data)
					dir[fileName] = refs
				}
				m.Unlock()

				c.WriteLine(fmt.Sprintf("OK r%d", len(refs)))

			case "get":
				if len(args) != 2 && len(args) != 3 {
					c.WriteLine("ERR usage: GET file [revision]")
					continue
				}
				dirPath, fileName, ok := parseFilePath(args[1])
				if !ok {
					c.WriteLine("ERR illegal file name")
					continue
				}
				var (
					hasRev bool
					revNo  int
				)
				if len(args) == 3 {
					revStr := strings.TrimPrefix(args[2], "r")
					revNo, err = strconv.Atoi(revStr)
					if err != nil {
						revNo = 0
					}
					hasRev = true
				}

				m.RLock()
				dir, ok := dirs[dirPath]
				var revs []string
				if ok {
					revs, ok = dir[fileName]
				}
				if !ok {
					m.RUnlock()
					c.WriteLine("ERR no such file")
					continue
				}
				if !hasRev {
					revNo = len(revs)
				}
				if revNo < 1 || revNo > len(revs) {
					m.RUnlock()
					c.WriteLine("ERR no such revision")
					continue
				}
				data := revs[revNo-1]
				m.RUnlock()

				c.WriteLine(fmt.Sprintf("OK %d", len(data)))
				c.WriteAll([]byte(data))

			default:
				c.WriteLine(fmt.Sprintf("ERR illegal method: %s", cmd))
				return nil
			}
		}
		return nil
	})
}
