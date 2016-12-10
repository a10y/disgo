// disGo: run a binary with different arguments on multiple possible host servers
// If there's a failure, it re-runs the binary, otherwise it writes the output
// to an attempt file.
package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"os"
	"os/exec"
	"time"
)

func debug(format string, args ...interface{}) {
	fullFormat := fmt.Sprintf("%v\n", format)
	fmt.Fprintf(os.Stderr, fullFormat, args...)
}

// Channel to communicate back on
func tryCommand(remoteCommand string, host string, outf io.Writer) error {
	cmd := exec.Command("ssh", host, remoteCommand)
	cmd.Stdout = outf
	cmd.Stderr = outf
	err := cmd.Run()
	if err != nil {
		return err
	}
	return nil
}

// Dispatch a given command to one of a set of available servers. If the command fails,
// attempt to try it again on a different server.
func dispatch(id int, command string, hosts []string, doneChan chan bool) {
	// Try hosts in a random order until one works
	order := rand.Perm(len(hosts))
	attempts := 0
	for _, i := range order {
		host := hosts[i]
		// Write out an attempt file for this command
		attemptOutputPath := fmt.Sprintf("cmd%v_attempt%v.log", id, attempts)
		outf, err := os.Create(attemptOutputPath)
		if err != nil {
			// Not sure how to recover from this, likely the FS is damaged or OOS.
			panic(err)
		}
		debug("EXEC command id=%v host=%v", id, host)
		if err := tryCommand(command, host, outf); err != nil {
			debug("ERROR id=%v status=%v", id, err)
			debug("Retrying command on next host...")
			continue
		}
		doneChan <- true
		return
	}
	debug("FAILED id=%v exhausted all servers and could not complete", id)
	doneChan <- false
}

// Read all lines from a file
func readLines(path string) ([]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	scanner := bufio.NewScanner(f)
	var lines []string
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	return lines, scanner.Err()
}

// Arguments to commands
var (
	cmdsFilePath  string
	hostsFilePath string
)

func main() {
	flag.StringVar(&cmdsFilePath, "cmds", "cmds.txt", "Files with commands to run, one per line")
	flag.StringVar(&hostsFilePath, "hosts", "hosts.txt", "Path to hosts file")
	flag.Parse()

	rand.Seed(time.Now().UnixNano())

	if len(os.Args) > 1 && os.Args[1] == "help" {
		flag.Usage()
		return
	}

	// Load commands and hosts, run all the items until completion
	commands, err := readLines(cmdsFilePath)
	if err != nil {
		panic(err)
	}

	hosts, err := readLines(hostsFilePath)
	if err != nil {
		panic(err)
	}

	// Try each command one at a time
	doneChan := make(chan bool)
	for i, cmd := range commands {
		go dispatch(i, cmd, hosts, doneChan)
	}

	// Wait for all to report in
	numCommands := len(commands)
	for left := 0; left < numCommands; left++ {
		<-doneChan // Do nothing here, maybe it'd be better to have something else eg. print out the number of failed commands.
	}
	debug("FINISHED %v commands", numCommands)
}
