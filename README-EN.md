<!-- 
Copyright (c) 2023- All neurodb_dart_driver authors. All rights reserved.

This source code is licensed under Apache 2.0 License.
-->

<h1 align="center"> Neurodb Dart Driver </h1>
<p align="center">
  <br> <a href="https://gitee.com/dudu-ltd/neurodb_dart_driver/README.md">中文</a> | English
</p>


<p align="center">
  <a title="Powered by Flame" href="https://pub.dev/packages/neurodb_dart_driver" >
      <img src="https://img.shields.io/badge/Pub-v0.0.1-red?style=popout" />
  </a>
  <a href="https://github.com/dudu-ltd/neurodb_dart_driver/stargazers">
      <img src="https://img.shields.io/github/stars/dudu-ltd/neurodb_dart_driver" alt="GitHub stars" />
  </a>
  <a href="https://github.com/dudu-ltd/neurodb_dart_driver/network/members">
      <img src="https://img.shields.io/github/forks/dudu-ltd/neurodb_dart_driver" alt="GitHub forks" />
  </a>
</p>

<p align="center">Neurodb driver writen by dart for flutter.</p>

---

### v0.0.1 version translated from:
- [neurodb-python-driver](https://github.com/pangguoming/neurodb-python-driver/)
- [neurodb-java-driver](https://github.com/pangguoming/neurodb-java-driver)

## Features

- [x] Support for `neurodb` 0.0.1 which DATABASE created by [https://github.com/pangguoming](https://github.com/pangguoming). [Home Page ->](https://neurodb.org/)

## Getting started

```sh
flutter pub add neurodb_dart_driver
```

## Usage

```dart
import 'package:neurodb_dart_driver/neurodb_dart_driver.dart';

void main() async {
  var driver = NeuroDBDriver("127.0.0.1", 8839);
  ResultSet resultSet = await driver.executeQuery("match (n) return n");
  resultSet = await driver.executeQuery("match (n)-[r]->(m) return n,r,m ");
  print("ok");
  driver.close();
}
```

## Licence

neurodb_dart_driver is under the [Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0).
