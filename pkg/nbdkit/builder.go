package nbdkit

type CompressionMethod string

const (
	NoCompression     CompressionMethod = "none"
	ZlibCompression   CompressionMethod = "zlib"
	FastLzCompression CompressionMethod = "fastlz"
	SkipzCompression  CompressionMethod = "skipz"
)
