package agent_pkg

import (
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
)

func copyDir(src string, dest string) error {
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
		Log.Error("copy dir failed when newWafInstance, src = %s, dest = %s", src, dest)
		return err
	}

	return nil
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
		//		fmt.Println(err.Error())
		return
	}
	defer srcFile.Close()
	//	fmt.Println("dst:" + dst)
	dst_slices := strings.Split(dst, "/")
	dst_slices_len := len(dst_slices)
	dest_dir := ""
	for i := 0; i < dst_slices_len-1; i++ {
		dest_dir = dest_dir + dst_slices[i] + "/"
	}
	//	fmt.Println("dest_dir:" + dest_dir)
	b, err := PathExists(dest_dir)
	if b == false {
		err := os.Mkdir(dest_dir, os.ModePerm)
		if err != nil {
			//			fmt.Println(err)
		}
	}
	dstFile, err := os.Create(dst)

	if err != nil {
		//		fmt.Println(err.Error())
		return
	}

	defer dstFile.Close()

	return io.Copy(dstFile, srcFile)
}

func WriteFile(dir string, file string, bytes []byte) error {
	isExist, err := pathExists(dir)
	if !isExist {
		err := os.MkdirAll(dir, 0777)
		if err != nil {
			Log.Error("mkdir err %s", dir)
			return err
		} else {
		}
	}

	f, err := os.Create(dir + "/" + file)
	if nil != err {
		Log.Error("create file %s failed", dir+"/"+file)
		return err
	}

	defer f.Close()

	err = ioutil.WriteFile(dir+"/"+file, bytes, 0644)
	if nil != err {
		Log.Error("write file %s failed", dir+"/"+file)
		return err
	}

	return nil
}

func AppendWr(file string, content string) error {
	f, err := os.OpenFile(file, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if nil != err {
		Log.Error("open file %s failed", file)
		return err
	}
	_, err = io.WriteString(f, content)
	if nil != err {
		Log.Error("write file %s failed", file)
		return err
	}

	return nil
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
