package agent_pkg

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
)

func copyDir(src string, dest string) {
	src_original := src
	err := filepath.Walk(src, func(src string, f os.FileInfo, err error) error {
		if f == nil {
			return err
		}
		if f.IsDir() {
		} else {
			dest_new := strings.Replace(src, src_original, dest, -1)
			CopyFile(src, dest_new)
		}
		return nil
	})
	if err != nil {
		fmt.Printf("filepath.Walk() returned %v\n", err)
	}
}

func getFilelist(path string) {
	err := filepath.Walk(path, func(path string, f os.FileInfo, err error) error {
		if f == nil {
			return err
		}
		if f.IsDir() {
			return nil
		}
		println(path)
		return nil
	})
	if err != nil {
		fmt.Printf("filepath.Walk() returned %v\n", err)
	}
}
func PathExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

func CopyFile(src, dst string) (w int64, err error) {
	srcFile, err := os.Open(src)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	defer srcFile.Close()
	fmt.Println("dst:" + dst)
	dst_slices := strings.Split(dst, "/")
	dst_slices_len := len(dst_slices)
	dest_dir := ""
	for i := 0; i < dst_slices_len-1; i++ {
		dest_dir = dest_dir + dst_slices[i] + "/"
	}
	fmt.Println("dest_dir:" + dest_dir)
	b, err := PathExists(dest_dir)
	if b == false {
		err := os.Mkdir(dest_dir, os.ModePerm)
		if err != nil {
			fmt.Println(err)
		}
	}
	dstFile, err := os.Create(dst)

	if err != nil {
		fmt.Println(err.Error())
		return
	}

	defer dstFile.Close()

	return io.Copy(dstFile, srcFile)
}

func WriteFile(dir string, file string, bytes []byte) bool {
	fmt.Println("WriteFile func")
	isExist, err := pathExists(dir)
	if !isExist {
		err := os.MkdirAll(dir, 0777)
		if err != nil {
			fmt.Printf("%s", err)
		} else {
			fmt.Print("Create Directory OK!")
		}
	}

	f, err := os.Create(dir + "/" + file)
	if nil != err {
		fmt.Println(err.Error())
	}

	defer f.Close()

	ok := true
	err = ioutil.WriteFile(dir+"/"+file, bytes, 0644)
	if nil != err {
		ok = false
	}

	return ok
}

func AppendWr(file string, content string) {
	f, err := os.OpenFile(file, os.O_WRONLY|os.O_APPEND, 0666)
	if nil != err {
		fmt.Printf("Open File %s Err", file)
	}
	_, err = io.WriteString(f, content)
	if nil != err {
		fmt.Printf("Write File %s Err", file)
	}
}

func pathExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}

	return false, err
}
