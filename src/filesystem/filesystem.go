package filesystem

import "HyDFS/src/membership"

const (
	MAX_FILE_NUM = 1000
)

type File struct {
	filename string
}

type FileServer struct {
	aliveml membership.MembershipList
	files   [MAX_FILE_NUM]File
	id      int
}

func FileServerInit(id int) *FileServer {
	return &FileServer{
		id: id,
	}
}

func (fs *FileServer) GetId() int {
	return fs.id
}
