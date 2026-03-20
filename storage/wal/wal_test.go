package wal

import (
	"os"
	"path/filepath"
	"testing"
)

func tempDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "w-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	return dir
}

// 새로운 레코드를 추가하고 모든 레코드를 읽어오는 테스트
func TestAppendAndReadAll(t *testing.T) {
	dir := tempDir(t)
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	// 100개의 레코드 기록
	for i := 0; i < 100; i++ {
		data := []byte("record-" + string(rune('A'+i%26)))
		if err := w.Append(data); err != nil {
			t.Fatal(err)
		}
	}

	// 모든 레코드 읽어오기
	records, err := w.ReadAll()
	if err != nil {
		t.Fatal(err)
	}

	if len(records) != 100 {
		t.Fatalf("Expected 100 records, got %d", len(records))
	}
	w.Close()
}

// 파일을 닫고 다시 열어도 레코드가 유지되는지 테스트
func TestSurvivesReopen(t *testing.T) {
	dir := tempDir(t)

	// 레코드 기록 후 닫기
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	w.Append([]byte("hello"))
	w.Append([]byte("world"))
	w.Close()

	// 다시 파일을 열고 읽어오기
	w2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	records, err := w2.ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 2 {
		t.Fatalf("Expected 2 records after reopen, got %d", len(records))
	}
	if string(records[0]) != "hello" || string(records[1]) != "world" {
		t.Fatal("data mismatch after reopen")
	}
	w2.Close()
}

// 파일 중간에 잘린 데이터가 있는 경우 무시되는지 테스트
func TestTruncatedWriteIsSkipped(t *testing.T) {
	dir := tempDir(t)

	w, _ := Open(dir)
	w.Append([]byte("some-record"))
	w.Close()

	// 파일 중간에 잘린 데이터 추가
	f, _ := os.OpenFile(filepath.Join(dir, "w.log"), os.O_APPEND|os.O_WRONLY, 0644)
	f.Write([]byte{0xFF, 0xFF}) // 헤더 부분에 잘린 데이터 추가
	f.Close()

	// 다시 파일을 열고 읽어오기
	w2, _ := Open(dir)
	records, _ := w2.ReadAll()
	if len(records) != 1 {
		t.Fatalf("Expected 1 record after truncated write, got %d", len(records))
	}
	w2.Close()
}

// 스냅샷을 생성한 뒤 로그 파일을 초기화하는 테스트
func TestReset(t *testing.T) {
	dir := tempDir(t)

	w, _ := Open(dir)
	w.Append([]byte("before-reset"))
	w.Reset()
	w.Append([]byte("after-reset"))

	records, _ := w.ReadAll()
	if len(records) != 1 {
		t.Fatalf("Expected 1 record after reset, got %d", len(records))
	}
	if string(records[0]) != "after-reset" {
		t.Fatal("data mismatch after reset")
	}
	w.Close()
}
