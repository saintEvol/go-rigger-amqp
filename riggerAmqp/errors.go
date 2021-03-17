package riggerAmqp

import "fmt"

// 找不到进程ID
type  ErrorFindNoPid string

func (e ErrorFindNoPid) Error() string {
	return fmt.Sprintf("faild to find Pid of: %s", string(e))
}

type ErrorConnInvalid string

func (e ErrorConnInvalid) Error() string {
	return "connection is invalid"
}
