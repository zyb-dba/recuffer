package utils

func SliceDelStr(sli []string, ele string) []string {
	for i := 0; i < len(sli); i++ {
		if sli[i] == ele {
			if i == 0 {
				sli = sli[1:]
			} else if i == len(sli)-1 {
				sli = sli[:i]
			} else {
				sli = append(sli[:i], sli[i+1:]...)
			}
			i--
		}
	}
	return sli
}
