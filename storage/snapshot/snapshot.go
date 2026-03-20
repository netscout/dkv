package snapshot

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"google.golang.org/protobuf/proto"

	pb "dkv/transport/proto/dkvpb"
)

// 스냅샷 데이터를 파일에 저장하는 함수: 임시 파일 생성 -> 동기화 -> 이름 변경
func Save(dir string, data *pb.SnapshotData) error {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("snapshot: failed to create directory: %w", err)
	}

	bytes, err := proto.Marshal(data)
	if err != nil {
		return fmt.Errorf("snapshot: failed to marshal snapshot data: %w", err)
	}

	filename := fmt.Sprintf("snapshot-%020d.snap", data.LastIndex)
	tmpPath := filepath.Join(dir, filename+".tmp")
	finalPath := filepath.Join(dir, filename)

	if err := os.WriteFile(tmpPath, bytes, 0644); err != nil {
		return fmt.Errorf("snapshot: failed to write temporary file: %w", err)
	}

	f, err := os.Open(tmpPath)
	if err != nil {
		return fmt.Errorf("snapshot: failed to open temporary file: %w", err)
	}
	// os.WriteFile로 파일을 생성할 경우, 디스크가 아닌 OS의 메모리 버퍼에만 존재할 수도 있음.
	// 따라서, 디스크에 쓰기 완료를 보장하기 위해 명시적으로 동기화 필요.
	if err := f.Sync(); err != nil {
		f.Close()
		return fmt.Errorf("snapshot: failed to sync temporary file: %w", err)
	}
	f.Close()

	return os.Rename(tmpPath, finalPath)
}

// 가장 최근의 스냅샷 데이터를 읽어오는 함수
// 스냅샷이 존재하지 않는 경우 nil, nil 반환
func Latest(dir string) (*pb.SnapshotData, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("snapshot: failed to read directory: %w", err)
	}

	// .snap 파일을 찾아서, 이름으로 정렬
	var snapFiles []string
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".snap") {
			snapFiles = append(snapFiles, e.Name())
		}
	}
	if len(snapFiles) == 0 {
		return nil, nil
	}
	sort.Strings(snapFiles)

	// 가장 최근의 스냅샷 파일을 읽어오기
	path := filepath.Join(dir, snapFiles[len(snapFiles)-1])
	bytes, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("snapshot: failed to read snapshot file: %w", err)
	}

	data := &pb.SnapshotData{}
	if err := proto.Unmarshal(bytes, data); err != nil {
		return nil, fmt.Errorf("snapshot: failed to unmarshal snapshot data: %w", err)
	}

	return data, nil
}
