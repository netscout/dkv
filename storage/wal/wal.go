package wal

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
)

// 다음과 같은 구조로 디스크에 저장:
// [4 바이트: 페이로드 길이] [4 바이트: CRC32 체크섬] [N 바이트: 페이로드 내용]

var (
	ErrCorrupted = errors.New("wal: corrupted record")
)

type WAL struct {
	mu   sync.Mutex
	file *os.File
	dir  string
}

// WAL 파일을 열고 초기화하는 함수
func Open(dir string) (*WAL, error) {
	//0755: 현재 사용자와 그룹에게 읽기/쓰기 권한, 다른 사용자에게는 읽기 권한만 부여
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	path := filepath.Join(dir, "wal.log")
	//0644: 현재 사용자에게 읽기/쓰기 권한, 그룹과 다른 사용자에게는 읽기 권한만 부여
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}
	return &WAL{
		file: f,
		dir:  dir,
	}, nil
}

// 새로운 레코드를 WAL 파일에 추가하는 함수
func (w *WAL) Append(payload []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	header := make([]byte, 8)
	binary.LittleEndian.PutUint32(header[0:4], uint32(len(payload)))
	binary.LittleEndian.PutUint32(header[4:8], crc32.ChecksumIEEE(payload))

	// 8바이트 헤더(페이로드 길이와 CRC32 체크섬) 쓰기
	if _, err := w.file.Write(header); err != nil {
		return err
	}
	// 페이로드 쓰기
	if _, err := w.file.Write(payload); err != nil {
		return err
	}
	// 디스크에 쓰기 완료
	return w.file.Sync()
}

// 모든 레코드를 읽어오는 함수
func (w *WAL) ReadAll() ([][]byte, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	// 전체 검색을 위해 파일의 시작점으로 이동
	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}

	var records [][]byte
	header := make([]byte, 8)

	for {
		// 8바이트 헤더(페이로드 길이와 CRC32 체크섬) 읽기
		if _, err := io.ReadFull(w.file, header); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				break // 파일 끝에 도달했거나, 헤더 쓰기 실패가 발생한 경우
			}
			return nil, err
		}

		// 4바이트 -> 페이로드 길이로 변환
		length := binary.LittleEndian.Uint32(header[0:4])
		// 4바이트 -> CRC32 체크섬으로 변환
		checksum := binary.LittleEndian.Uint32(header[4:8])

		// 페이로드의 길이만큼 메모리 할당 후 파일에서 읽기
		payload := make([]byte, length)
		if _, err := io.ReadFull(w.file, payload); err != nil {
			break // 페이로드 읽기 실패시, 여기서 종료
		}

		if crc32.ChecksumIEEE(payload) != checksum {
			break //오염된 기록이 발견되었으니 여기서 종료!
		}

		records = append(records, payload)
	}

	// 나중에 계속 추가될 항목을 위해 파일의 끝 부분으로 이동
	if _, err := w.file.Seek(0, io.SeekEnd); err != nil {
		return nil, err
	}

	return records, nil
}

// 스냅샷을 생성한 뒤 로그 파일을 초기화하는 함수
func (w *WAL) Reset() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.file.Truncate(0); err != nil {
		return err
	}
	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return err
	}
	return w.file.Sync()
}

// WAL 파일을 닫는 함수
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	return w.file.Close()
}
