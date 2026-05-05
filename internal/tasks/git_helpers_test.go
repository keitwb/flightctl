package tasks

import (
	"context"
	"errors"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/go-git/go-billy/v5/memfs"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/stretchr/testify/require"
)

var _ = Describe("ConvertFileSystemToIgnition", func() {
	When("the path is a directory", func() {
		It("converts the directory to an ignition config with subdirectory and files", func() {
			mfs := memfs.New()
			_ = mfs.MkdirAll("/testDAta/etc/testdir", 0755)
			files := []string{"/testdir/file1", "/file2"}
			sort.Slice(files, func(i, j int) bool { return strings.ToLower(files[i]) < strings.ToLower(files[j]) })

			file1, _ := mfs.Create(filepath.Join("/testDAta", files[0]))
			_, _ = file1.Write([]byte("content1"))
			file2, _ := mfs.Create(filepath.Join("/testDAta", files[1]))
			_, _ = file2.Write([]byte("content2"))

			ignitionConfig, err := ConvertFileSystemToIgnition(mfs, "/testDAta")
			Expect(err).ToNot(HaveOccurred())
			Expect(ignitionConfig.Storage.Files).To(HaveLen(2))

			sort.Slice(ignitionConfig.Storage.Files, func(i, j int) bool {
				return strings.ToLower(ignitionConfig.Storage.Files[i].Path) < strings.ToLower(ignitionConfig.Storage.Files[j].Path)
			})
			Expect(ignitionConfig.Storage.Files[0].Path).To(Equal(filepath.Join("/etc/", files[0])))
			Expect(ignitionConfig.Storage.Files[1].Path).To(Equal(filepath.Join("/etc/", files[1])))
		})
	})

	When("the path is a file in non-slash folder", func() {
		It("converts the file to an ignition config", func() {
			mfs := memfs.New()
			path := "/somefolder/testfile"
			file, _ := mfs.Create(path)
			_, _ = file.Write([]byte("content"))

			ignitionConfig, err := ConvertFileSystemToIgnition(mfs, path)
			Expect(err).ToNot(HaveOccurred())
			Expect(ignitionConfig.Storage.Files).To(HaveLen(1))
			Expect(ignitionConfig.Storage.Files[0].Path).To(Equal("/etc/testfile"))
		})
	})

	When("the path is a file in / folder", func() {
		It("converts the file to an ignition config", func() {
			mfs := memfs.New()
			file, _ := mfs.Create("/testfile")
			_, _ = file.Write([]byte("content"))

			ignitionConfig, err := ConvertFileSystemToIgnition(mfs, "/testfile")
			Expect(err).ToNot(HaveOccurred())
			Expect(ignitionConfig.Storage.Files).To(HaveLen(1))
			Expect(ignitionConfig.Storage.Files[0].Path).To(Equal("/testfile"))
		})
	})

	When("the path does not exist", func() {
		It("returns an error", func() {
			mfs := memfs.New()

			_, err := ConvertFileSystemToIgnition(mfs, "/nonexistent")
			Expect(err).To(HaveOccurred())
		})
	})

	When("the path is empty", func() {
		It("returns an error", func() {
			mfs := memfs.New()

			_, err := ConvertFileSystemToIgnition(mfs, "")
			Expect(err).To(HaveOccurred())
		})
	})
})

func TestSanitizeGitError(t *testing.T) {
	require := require.New(t)
	tests := []struct {
		name     string
		input    error
		expected string
	}{
		{
			name:     "strips prefix before colon",
			input:    errors.New("authentication required: invalid credentials"),
			expected: "invalid credentials",
		},
		{
			name:     "strips trailing period",
			input:    errors.New("connection refused."),
			expected: "connection refused",
		},
		{
			name:     "strips prefix and trailing period",
			input:    errors.New("authentication required: token expired."),
			expected: "token expired",
		},
		{
			name:     "passes through simple message",
			input:    errors.New("timeout"),
			expected: "timeout",
		},
		{
			name:     "uses last colon for nested errors",
			input:    errors.New("transport: auth: bad token https://user:pass@host.com"),
			expected: "bad token https://user:pass@host.com",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := sanitizeGitError(tt.input)
			require.Equal(tt.expected, result)
		})
	}
}

func TestGitLsRemote_EmptyURL(t *testing.T) {
	require := require.New(t)
	_, err := GitLsRemote(context.Background(), "", "main", nil)
	require.Error(err)
	require.Contains(err.Error(), "must not be empty")
}

func TestGitLsRemote_InvalidURL(t *testing.T) {
	require := require.New(t)
	_, err := GitLsRemote(context.Background(), "not-a-real-url://invalid", "main", nil)
	require.Error(err)
	require.Contains(err.Error(), "failed to list remote refs")
	require.NotContains(err.Error(), "password")
	require.NotContains(err.Error(), "token")
}
