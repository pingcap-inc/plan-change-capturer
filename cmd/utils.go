package cmd

func compareVer(ver1, ver2 string) int {
	if ver1 < ver2 {
		return -1
	} else if ver1 == ver2 {
		return 0
	}
	return 1
}
