package scenarios

type Scenarios interface {
	Run() error
	Validate() error
}
