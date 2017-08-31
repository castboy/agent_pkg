package agent_pkg

import (
	"fmt"
	"os/exec"
)

func execCommand(commandName string, params []string) bool {
	cmd := exec.Command(commandName, params...)

	_, err := cmd.CombinedOutput()
	if err != nil {
		fmt.Println(err)

		return false
	}

	return true
}
