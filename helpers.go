package recipes

import (
	"strings"

	"github.com/coreos/go-etcd/etcd"
)

var testClient = etcd.NewClient([]string{"http://localhost:4001"})

func addPrefix(key string) string {
	if !strings.HasPrefix(key, "/") {
		return "/" + key
	}

	return key
}
