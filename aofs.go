package aofs

type File interface {
	Write(p []byte) (n int, err error)
	Flush() error
	Close() error
}

type FileSystem interface {
	Open(name string) (File, error)
}
