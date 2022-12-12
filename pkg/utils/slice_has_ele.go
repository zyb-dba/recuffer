package utils

func SliceHasStr(str string, sli []string) bool {
	if len(sli) > 0 {
		for _, v := range sli {
			if str == v {
				return true
			}
		}
	}
	return false
}
