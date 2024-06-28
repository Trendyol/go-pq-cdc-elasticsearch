package bytes

import "testing"

func TestMultiDimByteReader(t *testing.T) {
	reader := NewMultiDimReader(nil)
	b := make([]byte, 10)
	n, err := reader.Read(b)

	if n != 0 || err == nil {
		t.Error("Expected 0, io.EOF")
	}

	c := make([][]byte, 2)
	c[0] = []byte("hello")
	c[1] = []byte("world")
	reader = NewMultiDimReader(c)

	b = make([]byte, 3)
	n, err = reader.Read(b)

	if n != 3 || err != nil {
		t.Error("Expected 3, nil")
	}

	d := make([]byte, 8)
	n, err = reader.Read(d)
	if n != 2 || err != nil {
		t.Error("Expected 2, nil")
	}
}
