package agent_pkg

import (
	"os/exec"
)

func execCommand(commandName string, params []string) error {
	cmd := exec.Command(commandName, params...)

	_, err := cmd.CombinedOutput()
	if err != nil {
		return err
	}

	return nil
}
