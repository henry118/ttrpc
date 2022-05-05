/*
   Copyright The containerd Authors.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package main

import (
	"google.golang.org/protobuf/compiler/protogen"
)

func main() {
	var servicePrefix string
	protogen.Options{
		ParamFunc: func(name, value string) error {
			if name == "prefix" {
				servicePrefix = value
			}
			return nil
		},
	}.Run(func(gen *protogen.Plugin) error {
		for _, f := range gen.Files {
			if !f.Generate {
				continue
			}
			if err := generate(gen, f, servicePrefix); err != nil {
				return err
			}
		}
		return nil
	})
}
