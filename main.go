package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"github.com/Priyanshu23/FlashLogGo/kv"
)

func main() {
	db, err := kv.NewKV()
	if err != nil {
		fmt.Println("failed to start db:", err)
		os.Exit(1)
	}

	fmt.Println("Simple KV CLI")
	fmt.Println("Commands:")
	fmt.Println("  put <key> <value>")
	fmt.Println("  get <key>")
	fmt.Println("  delete <key>")
	fmt.Println("  exit")

	scanner := bufio.NewScanner(os.Stdin)

	for {
		fmt.Print("> ")

		if !scanner.Scan() {
			break
		}

		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		parts := strings.Fields(line)
		cmd := strings.ToLower(parts[0])

		switch cmd {

		case "put":
			if len(parts) < 3 {
				fmt.Println("usage: put <key> <value>")
				continue
			}
			key := parts[1]
			value := strings.Join(parts[2:], " ")

			if err := db.Put(key, []byte(value)); err != nil {
				fmt.Println("error:", err)
				continue
			}
			fmt.Println("ok")

		case "get":
			if len(parts) != 2 {
				fmt.Println("usage: get <key>")
				continue
			}
			key := parts[1]

			val, found := db.Get(key)
			if !found {
				fmt.Println("(nil)")
				continue
			}
			fmt.Println(string(val))

		case "delete":
			if len(parts) != 2 {
				fmt.Println("usage: delete <key>")
				continue
			}
			key := parts[1]

			db.Delete(key)
			fmt.Println("ok")

		case "exit", "quit":
			fmt.Println("bye 👋")
			return

		default:
			fmt.Println("unknown command:", cmd)
		}
	}

	if err := scanner.Err(); err != nil {
		fmt.Println("input error:", err)
	}
}
